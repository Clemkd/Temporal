# Résultats des tests

Environnement : conteneur Linux 4 vCPU / 15 Go, Temporal 1.x (auto-setup sur Postgres 17), SeaweedFS (S3), Postgres 17,
.NET 10.0, Temporalio 1.19. Tout tourne sur la même machine : les débits sont donc des minima.

## Tableau de synthèse

| # | Test | Défaillances injectées | Caractéristiques | Résultat obtenu | État | Raison de l'échec / perte associée |
|---|---|---|---|---|---|---|
| 1 | Tests unitaires et de workflows (`dotnet test`) | Erreurs non retryables, erreurs transitoires, erreur persistante, pipeline inexistant, annulation | 29 tests : parsing et validation, catégorisation, store S3 local, workflows sur un serveur Temporal local avec activités simulées | 29/29 passés | ✅ Passé | Premier passage : 1 échec qui a révélé un vrai bug (`FileSystemObjectStore.DeleteAsync` levait une exception si le dossier des tags n'existait pas). Corrigé. Aucune perte : bug local au stockage fichier, pas au S3. |
| 2 | Fonctionnel : 200 fichiers | 5 % de fichiers au contenu invalide | 200 fichiers × 200 lignes, CSV et JSON | 191 dans `processed/`, 9 dans `invalid/` (tags `status/reason/step`, rapport `.error.json`, statut en base), 0 restant | ✅ Passé | — |
| 3 | Fichiers « poison » | Erreur retryable systématique à l'étape `convert` | 6 fichiers dont 4 poison | Les 4 poison sont en quarantaine après 5 tentatives (`IOException`), les 2 valides sont stockés | ✅ Passé | — |
| 4 | Traitements sur données stockées | Pause, update invalide (batch 20000), changement de taille de lot à chaud | 5 jobs en parallèle (un par type de capteur), lots de 200, continue-as-new tous les 5 lots | 38 300 mesures catégorisées, résultats insérés, update invalide rejetée par le validateur, pause et reprise OK | ✅ Passé | — |
| 5 | Chaos run 1 | SIGKILL d'une instance toutes les ~12 s (14 kills), redémarrage du serveur Temporal, 5 % d'erreurs transitoires, job de traitement lancé pendant l'ingestion | 1000 fichiers × 100 lignes, 3 instances | 1000/1000 consommés : 945 stockés, 55 invalides (= 50 invalides + 5 poison générés). 94 500 mesures = attendu, 0 doublon, 0 mesure orpheline, tout catégorisé. Débit ≈ 17 500 fichiers/h | ✅ Passé | Anomalie sans perte : 3 démarrages d'instance ont échoué pendant le redémarrage de Temporal (le worker s'arrêtait faute de serveur). Le superviseur les a relancés. Corrigé : le bootstrap attend Temporal avant de démarrer les workers (confirmé au run 3). |
| 6 | Chaos run 2 (plus sévère) | SIGKILL toutes les ~10 s (24 kills), redémarrage de Temporal, **10 %** d'erreurs transitoires | 1000 fichiers × 100 lignes | 1000/1000 consommés, 0 doublon, 0 perte de mesure pour les fichiers stockés. **2 contrôles en échec** | ❌ Échoué | **a) 1 fichier valide mis en quarantaine** (`file-000031.csv`) : l'étape `validate` n'autorisait que 3 tentatives. Avec 10 % d'erreurs injectées plus les kills (une tentative tuée compte comme une tentative), les 3 ont échoué pour des raisons purement techniques. **Perte : 1 fichier (100 mesures) non intégré**, mais pas perdu : il est dans `invalid/` avec la raison `TransientChaosException` et peut être relancé via `/api/files/reprocess`. (Un 2e fichier, déjà invalide par son contenu, a été étiqueté avec la raison technique au lieu de la vraie : bon résultat, mauvaise raison.) **b) Table de staging non vide** : 50 lignes laissées par un job annulé manuellement *avant* ce test (démo « reload »). Une annulation entre `categorize` et `cleanup` ne nettoyait pas le staging. Perte : aucune, données temporaires uniquement. |
| 7 | Chaos run 3 (mêmes conditions que le run 2, après correctifs) | SIGKILL toutes les ~10 s (18 kills), redémarrage de Temporal, 10 % d'erreurs transitoires | 1000 fichiers × 100 lignes | 12/12 contrôles OK, dont le nouveau « aucun fichier en quarantaine pour erreur uniquement technique ». 0 instance tombée d'elle-même. Débit ≈ 17 000 fichiers/h | ✅ Passé | Correctifs appliqués : `MaxAttempts` à 10 par défaut (le contenu invalide est non retryable, donc la limite ne sert qu'aux erreurs techniques), nettoyage du staging à la fin ou à l'échec d'un job, contrôle du staging limité au test, endpoint `POST /api/pipelines/{name}/reset`. |
| 8 | File de tâches qui grossit | 1 seul worker bridé (2 activités et 2 workflow tasks en parallèle), puis +3 workers à T+60 s | Rafale de 3000 fichiers × 50 lignes | Phase bridée : ~700 fichiers/h, backlog de workflow tasks jusqu'à ~2 100. Après scale-out : 60 000 à 77 000 fichiers/h, backlog résorbé, 3000/3000 consommés en 427 s | ✅ Passé | — |
| 9 | Kill pendant une étape longue | SIGKILL au milieu d'une étape de 20 s (heartbeat timeout 5 s) | Pipeline modifié à chaud (v2, ajout de `ingest.delay`) | Tentative 1 tuée à ~11 s, détection 5 s plus tard, tentative 2 terminée en 12 s (reprise depuis le dernier heartbeat, pas depuis zéro). Fichier stocké avec la version de pipeline 2 | ✅ Passé | — |
| 10 | Changement de pipeline sur un job en cours | Signal `ReloadPipeline`, puis annulation | Job temperature, lots de 50 | Le job passe de v1 (4 étapes) à v2 (5 étapes, nouveaux seuils) sans redémarrer. Après annulation : workflow `Canceled` et job `Failed` en base | ✅ Passé | Cette annulation a laissé le staging non nettoyé qui a fait échouer le run 2 (b). Corrigé depuis. |

## Invariants vérifiés par `scripts/chaos_test.py`

1. `incoming/` et `processing/` vides : tout fichier est consommé.
2. `processed/` + `invalid/` = fichiers générés.
3. `invalid/` = invalides + poison générés.
4. Aucun fichier mis en quarantaine pour une raison purement technique (ajouté après le run 2).
5. Statuts en base = contenu du bucket.
6. Tous les fichiers poison sont en quarantaine.
7. Nombre de mesures = somme des lignes stockées = fichiers stockés × lignes : ni perte ni doublon.
8. Aucune mesure en double (`FileKey`, `LineNumber`).
9. Aucune mesure pour un fichier invalide (compensation).
10. Toutes les mesures sont catégorisées.
11. Tous les jobs de traitement sont terminés (`Completed`).
12. Table de staging vide.

## Enseignements

- La garantie « tout fichier est consommé » a tenu sur **tous** les runs, y compris celui en échec : aucun fichier n'est jamais resté dans `incoming/` ou `processing/`, et aucune mesure n'a été perdue ni dupliquée.
- La vraie limite est **métier** : il faut dimensionner les retries pour que des erreurs techniques transitoires ne fassent pas passer un fichier valide en « invalide ». Deux règles :
  1. le contenu invalide doit toujours lever une erreur **non retryable** ;
  2. `MaxAttempts` doit être large (ou illimité avec `ScheduleToClose`) pour le reste.

  Le rapport d'erreur et le tag `reason` permettent de distinguer et de relancer les faux invalides.
- Un process qui démarre pendant une coupure de Temporal ne doit pas dépendre du superviseur : le bootstrap attend maintenant le serveur.
- Toute donnée temporaire d'un job (staging) doit être nettoyée sur **tous** les chemins de sortie : succès, échec et annulation.
