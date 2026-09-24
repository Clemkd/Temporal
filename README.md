# POC ASP.NET 10 + Temporal : intégration de fichiers capteurs et traitements longs

Ce POC vérifie qu'on peut bâtir sur **Temporal** (SDK .NET `Temporalio` 1.19) une chaîne d'intégration
**résiliente aux crashs** : on tue un process à n'importe quel moment, on le redémarre, et tout reprend comme
s'il ne s'était jamais arrêté.

| Besoin | Réponse dans le POC |
|---|---|
| ~1000 fichiers/heure, un workflow d'intégration par fichier | `InboxWatcherWorkflow` scanne `incoming/` et démarre un `FileIngestionWorkflow` par fichier (id = `ingest:<clé>`, donc jamais deux fois en parallèle). Mesuré : **> 10 000 fichiers/h** sur un seul poste, sous chaos. |
| Étapes S3 → validation → conversion → stockage | Étapes `ingest.fetch`, `ingest.validate`, `ingest.convert`, `ingest.store`, encadrées par `claim` (déplacement `incoming/` → `processing/`) et `archive`/`quarantine`. |
| Traitements sur les données stockées | `SensorProcessingWorkflow` : récupération par type de capteur (lots, curseur), catégorisation, mise à jour des mesures, insertion des résultats. |
| Résilience crash/redémarrage | État event-sourcé par Temporal + activités idempotentes + heartbeats. Démontré par `scripts/chaos_test.py`. |
| Tout fichier consommé, étiqueté invalide si l'erreur persiste après les retries | Chaque fichier finit dans `processed/` ou `invalid/` (tags S3 + rapport `.error.json` + statut en base). La finalisation est retentée sans limite, et elle s'exécute même si le workflow est annulé. |
| Jobs longue durée | Watcher sans fin et jobs de traitement : *continue-as-new*, pause/reprise, requête de progression. |
| Étapes configurables au runtime | Pipelines versionnés en base, modifiables par l'API ; un job en cours peut recharger son pipeline à chaud. |
| Files de tâches qui grossissent | Files séparées (`file-ingestion`, `sensor-processing`, `control`), backlog exposé par `/api/stats`, workers ajoutés à chaud, limite `MaxInFlight` optionnelle. |

## Architecture

```
                 ┌───────────────────────── Temporal (Postgres) ─────────────────────────┐
                 │  control queue         file-ingestion queue       sensor-processing q. │
                 └──────▲──────────────────────────▲──────────────────────────▲───────────┘
                        │ poll                     │ poll                     │ poll
 ┌──────────────┐   ┌───┴──────────────────────────┴──────────────────────────┴──────┐
 │ API ASP.NET  │──▶│  N process identiques (même image) : API + workers hébergés    │
 │ /api/...     │   │  (Worker:Enabled, Worker:TaskQueues pour spécialiser)           │
 └──────────────┘   └───────┬─────────────────────────────────┬──────────────────────┘
                            │                                 │
                  S3 (SeaweedFS/MinIO/AWS)               PostgreSQL (données)
        incoming/ → processing/ → processed/ | invalid/   files, measurements, pipelines,
                     converted/ (NDJSON normalisé)         processing_jobs, processing_results
```

Le même binaire sert d'API et de worker. En production on sépare les rôles par configuration
(`Worker__Enabled`, `Worker__TaskQueues__0=file-ingestion`, `Watcher__AutoStart`) et on scale les workers
horizontalement : Temporal répartit les tâches entre tous les pollers.

### Projets

- `src/TemporalPoc.Core` : domaine (parsing/validation/conversion, catégorisation), stockage objet (S3 et système de fichiers), EF Core/Postgres, pipelines, **workflows** et **activités**, enregistrement DI.
- `src/TemporalPoc.Api` : hôte ASP.NET 10 (minimal APIs + OpenAPI), bootstrap (schéma, bucket, démarrage du watcher).
- `tests/TemporalPoc.Tests` : tests unitaires et tests de workflows sur un serveur Temporal local (activités simulées).
- `scripts/chaos_test.py` : test de bout en bout avec kills `SIGKILL` et redémarrage du serveur Temporal.

## Workflows

### `InboxWatcherWorkflow` (id fixe `inbox-watcher`, file `control`)
Boucle infinie : l'activité `dispatch.scan-inbox` liste `incoming/` et démarre un `FileIngestionWorkflow`
par fichier (`IdReusePolicy=AllowDuplicate`, `IdConflictPolicy=Fail` : un fichier déjà en cours est ignoré).
Toutes les 8 itérations, `dispatch.reconcile` redémarre les fichiers orphelins (restés dans `processing/` sans
workflow en cours, par exemple après un `terminate` manuel).
- Signaux `Poke` (scan immédiat, envoyé après un upload), `Pause` et `Resume`. Update validée `Configure` (intervalle, taille de scan, `MaxInFlight`, pipeline).
- Continue-as-new toutes les 200 itérations, ou quand Temporal le suggère : l'historique reste borné.
- Démarré au boot avec `IdConflictPolicy=UseExisting`. Plusieurs instances peuvent booter en même temps sans créer de doublon.

### `FileIngestionWorkflow` (un par fichier, file `file-ingestion`)
1. `ingest.claim` : déplacement idempotent `incoming/x` → `processing/x` et ligne `files` en base.
2. `pipeline.get` : lecture du pipeline courant (ou d'une version précise). Comme c'est une activité, la définition lue est **enregistrée dans l'historique** et le replay reste déterministe même si le pipeline change ensuite.
3. Étapes du pipeline, chacune avec sa propre politique de retry, son timeout, son heartbeat et ses paramètres :
   - `ingest.fetch` : téléchargement S3, SHA-256, détection du format, taille maximale.
   - `ingest.validate` : en-tête, types, unités, bornes physiques, dates. Contenu invalide → `ApplicationFailure` **non retryable** (`InvalidFile` / `InvalidRows`), seuil `maxInvalidRowRatio`.
   - `ingest.convert` : normalisation en unités canoniques (°F→°C, kPa→hPa…) et écriture de `converted/x.ndjson`.
   - `ingest.store` : dans une transaction, `DELETE` des lignes du fichier puis `COPY` binaire. Rejouer l'étape ne crée ni doublon ni perte.
   - `ingest.delay` : étape lente artificielle. Elle reprend depuis son dernier heartbeat après un crash.
4. Succès : `ingest.archive` déplace le fichier vers `processed/`, avec des tags (pipeline, lignes, checksum).
   Échec, y compris après épuisement des retries ou sur annulation : `ingest.quarantine` s'exécute. Il **compense** (supprime les mesures déjà insérées et le fichier converti), écrit `invalid/x.error.json`, déplace le fichier vers `invalid/` avec les tags `status=invalid`, `reason` et `step`, et passe le statut à `Invalid` en base.

Ces activités d'infrastructure (claim, archive, quarantine, pipeline.get) sont retentées **sans limite**, avec
un backoff plafonné. Un fichier ne peut donc pas rester bloqué dans un état intermédiaire. Query `Status` : phase, étape courante, étapes terminées, erreur.

### `SensorProcessingWorkflow` (un par type de capteur et par job, file `sensor-processing`)
Pour chaque lot, `processing.fetch-batch` renvoie une plage d'ids (pagination keyset par `SensorType`, `Id`).
Les étapes du pipeline s'exécutent ensuite sur ce lot :
`processing.categorize` (écrit dans `categorization_staging`), `processing.update-measurements`
(`UPDATE … FROM staging`), `processing.insert-results` (agrégats par capteur, remplacés à l'identique si l'étape est rejouée),
puis `processing.cleanup`. Les données ne transitent jamais par Temporal (pattern *claim-check*) : seules des
références de lot circulent.
- Seuils de catégorisation (`LOW`/`NORMAL`/`HIGH`/`CRITICAL`) passés en paramètres d'étape, par exemple `thresholds.temperature = "5;30;45"`.
- Signaux `Pause`, `Resume` et `ReloadPipeline` (bascule sur la dernière version au lot suivant). Update validée `SetBatchSize`. Query `Status`.
- Continue-as-new tous les `BatchesPerRun` lots, en transportant la progression, la taille de lot, l'état de pause et le pipeline résolu.
- Progression persistée dans `processing_jobs`. Une annulation marque le job `Failed` (activité exécutée hors du scope annulé).
- `ContinueOnError` par étape : le lot est ignoré et compté dans `FailedBatches` au lieu de faire échouer le job.

## Pourquoi c'est résilient

- **Aucun état en mémoire qui compte** : l'avancement des workflows est dans l'historique Temporal. Au redémarrage, un autre worker (ou le même) rejoue l'historique et reprend à l'étape en cours.
- **Activités idempotentes** : on peut toujours les rejouer après un effet de bord dont la complétion n'a pas été enregistrée. Déplacement S3 = copie puis suppression, qui tolère « source absente, destination présente ». Stockage = transaction delete + insert. Résultats et staging sont remplacés par `(job, lot)`.
- **Heartbeats** (timeout 20 s par défaut, configurable par étape) : un worker tué est détecté en quelques secondes et l'activité est replanifiée ailleurs, sans attendre le `StartToClose`.
- **Ids de workflow déterministes** : le scan et le démarrage peuvent être rejoués sans créer de doublon.
- **Arrêt propre** : sur SIGTERM, `GracefulShutdownTimeout` laisse finir les activités en cours. Sur SIGKILL, ce sont les heartbeats et les retries qui prennent le relais.
- **Indisponibilité de Temporal ou de la base** : les clients se reconnectent. Un worker qui s'arrête faute de serveur est relancé par l'orchestrateur (`restart: unless-stopped`). Le bootstrap attend la base et le stockage.

## Démarrage

Prérequis : Docker, SDK .NET 10.

```bash
# Infra : Postgres, SeaweedFS (S3), Temporal (auto-setup sur Postgres), Temporal UI (http://localhost:8080)
docker compose up -d postgres seaweedfs temporal temporal-ui

# Application (API + workers) sur http://localhost:5055
dotnet build -c Release
scripts/start-instance.sh api 5055
scripts/start-instance.sh worker1 5056 Watcher__AutoStart=false     # workers supplémentaires

# ou tout en conteneurs, avec 2 workers en plus de l'API
docker compose up -d --build
docker compose up -d --scale worker=4      # plus de workers quand le backlog grossit
```

Spec OpenAPI : `http://localhost:5055/openapi/v1.json`. Sans S3, on peut utiliser `Storage__Provider=FileSystem`.

### Scénarios

```bash
# 1000 fichiers d'un coup (5 % invalides, 1 % "poison" qui échouent toujours, 1 % lents)
curl -XPOST localhost:5055/api/simulation/files -H 'content-type: application/json' \
  -d '{"count":1000,"rowsPerFile":100,"invalidRatio":0.05,"poisonRatio":0.01,"slowRatio":0.01}'

curl localhost:5055/api/stats                         # objets par préfixe, statuts, backlog des files
curl "localhost:5055/api/files?status=Invalid"         # fichiers invalides avec étape et erreur
curl "localhost:5055/api/files/status?key=<clé>"       # base + état live du workflow + tags S3
curl -XPOST "localhost:5055/api/files/reprocess?key=<clé>"   # renvoie un invalide dans incoming/

# Upload manuel
curl -F "file=@mesures.csv" localhost:5055/api/files

# Pipelines modifiables au runtime (nouvelle version, validée contre le catalogue d'étapes)
curl localhost:5055/api/pipelines/catalog
curl -XPUT localhost:5055/api/pipelines/file-ingestion -H 'content-type: application/json' -d '{
  "comment": "tolère 10% de lignes invalides + étape lente",
  "steps": [
    {"activity":"ingest.fetch"},
    {"activity":"ingest.validate","parameters":{"maxInvalidRowRatio":"0.1"}},
    {"activity":"ingest.delay","parameters":{"seconds":"20"},"heartbeatTimeoutSeconds":5},
    {"activity":"ingest.convert","maxAttempts":3},
    {"activity":"ingest.store","maxAttempts":10,"timeoutSeconds":300}
  ]}'

# Traitements : un job par type de capteur
curl -XPOST localhost:5055/api/processing/jobs -H 'content-type: application/json' -d '{"jobId":"j1","batchSize":500}'
curl localhost:5055/api/processing/jobs/j1-temperature
curl -XPOST localhost:5055/api/processing/jobs/j1-temperature/pause
curl -XPUT  localhost:5055/api/processing/jobs/j1-temperature/batch-size/2000
curl -XPOST localhost:5055/api/processing/jobs/j1-temperature/reload-pipeline
curl -XPOST localhost:5055/api/processing/jobs/j1-temperature/resume
curl "localhost:5055/api/processing/jobs/j1-temperature/results?take=20"

# Watcher
curl localhost:5055/api/watcher
curl -XPUT localhost:5055/api/watcher/config -H 'content-type: application/json' \
  -d '{"scanIntervalSeconds":5,"maxFilesPerScan":500,"maxInFlight":200,"orphanAfterMinutes":5,"pipeline":"file-ingestion"}'

# Chaos
curl -XPUT localhost:5055/api/chaos -H 'content-type: application/json' -d '{"transientFailureRate":0.1,"poisonMarker":"poison","slowMarker":"slow","slowSeconds":30}'
curl -XPOST localhost:5055/api/chaos/crash          # kill brutal du process
```

## Tests

```bash
dotnet test                                   # unitaires + workflows (serveur Temporal local téléchargé,
                                              # ou TEMPORAL_CLI_PATH=/chemin/temporal)
scripts/chaos_test.py --files 1000 --rows 100 --kill-every 12 --restart-temporal
```

Le test de chaos démarre 3 instances avec 5 % d'erreurs transitoires injectées. Il dépose N fichiers, puis
**tue une instance au hasard (SIGKILL) toutes les ~12 s** et la relance, **redémarre le conteneur Temporal**,
et lance un job de traitement pendant l'ingestion. Il vérifie ensuite ces invariants :

- `incoming/` et `processing/` vides : tous les fichiers ont été consommés ;
- `processed/` + `invalid/` = nombre de fichiers générés, et `invalid/` = invalides + poison générés ;
- statuts en base cohérents avec le bucket ;
- nombre de mesures = somme des lignes stockées = fichiers valides × lignes : **ni perte ni doublon** ;
- aucune mesure pour un fichier invalide (compensation) ;
- toutes les mesures catégorisées, jobs terminés, table de staging vide.

Voir [RESULTS.md](RESULTS.md) pour les mesures obtenues.

## Points d'attention pour aller plus loin

- **Versioning du code des workflows** : modifier la logique d'un workflow en cours d'exécution demande `Workflow.Patched(...)` ou le Worker Versioning (build ids). Le POC montre la variante « configuration », avec des étapes versionnées en base.
- **Déclenchement** : le polling S3 peut être remplacé par des notifications S3 (SQS ou webhook) qui envoient `Poke` au watcher, ou démarrent directement le workflow du fichier (même id, donc idempotent).
- **Scan d'un backlog très grand** : le watcher retente le démarrage des fichiers déjà en cours mais pas encore « claimés ». C'est bon marché à 1000 fichiers/h. Pour des volumes 100 fois plus grands, il faudrait un claim par le dispatcher ou `MaxInFlight`.
- **Pagination keyset** : l'ordre des `Id` n'est pas garanti en cas d'insertions concurrentes lentes. Le traitement vise les mesures non catégorisées (`OnlyUncategorized`), et un job suivant rattrape les retardataires.
- **Observabilité** : ajouter `Temporalio.Extensions.OpenTelemetry` (traces et métriques SDK), et alerter sur `ApproximateBacklogAge`.
- **Temporal en production** : cluster ou Temporal Cloud. L'image `auto-setup` sert au développement uniquement.
