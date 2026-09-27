# Traitement par véhicule sur une plage de temps

Implémentation d'un workflow Temporal par véhicule qui traite des plages de jours demandées à tout moment.
Code : `src/TemporalPoc.Core/Vehicles/`. API : `src/TemporalPoc.Api/Endpoints/VehicleEndpoints.cs`.
Tests : `tests/TemporalPoc.Tests/Vehicle*.cs`.

## Exigences et réponses

| Exigence | Implémentation |
|---|---|
| Jusqu'à 1 500 véhicules | Un workflow par véhicule (`vehicle:<id>`), sur une task queue dédiée `vehicle-processing`. Un workflow qui attend ne consomme presque rien. |
| Demandes ponctuelles (fichier reçu) et massives (tout le parc) | Signal `FileReceived` et workflow `FleetRequest` : voir « Trois sources de demandes » ci-dessous. |
| Fichier temps réel : 1 traitement par minute au plus, journée d'exploitation courante seulement | Intervalle minimal de 1 min pour la journée courante. Un fichier d'un autre jour est traité comme une demande journalière. |
| Traitement journalier : 1 fois par heure au plus, par véhicule | Intervalle minimal de 1 h pour tout autre jour, quelle que soit la source (y compris massive). |
| Demandes à tout moment, sur des plages différentes ou qui se croisent | Chaque demande est ajoutée à la **file de jours** du véhicule : Update-With-Start (avec accusé de réception) ou Signal-With-Start (envoi en masse). Le workflow est démarré s'il ne tourne pas. |
| Traiter chaque jour au moins une fois | Chaque jour demandé entre dans la file et y reste jusqu'à son traitement, même à travers un continue-as-new. Un jour redemandé **pendant** son traitement est refait ensuite. Après une panne, Temporal reprend exactement là où c'était. |
| Pas de doublon pour un jour demandé plusieurs fois et pas encore traité | La file est un ensemble de **jours** (clé = date) et non de demandes. Un jour déjà en attente est fusionné : une seule entrée, la priorité la plus haute, la liste des demandes. Une demande renvoyée avec le même `RequestId` est ignorée. |
| Relance des erreurs transitoires | L'activité classe les erreurs (`FailureClassifier`) : réseau, timeout, base indisponible → `ApplicationFailure` relançable. |
| Pas de relance des erreurs métier ou applicatives | `VehicleBusinessException` → type `BusinessError`, non relançable. Toute autre exception (bug) → `ApplicationError`, non relançable. `NonRetryableErrorTypes` dans la RetryPolicy en plus, par sécurité. |
| Relance exponentielle avec jitter | La `RetryPolicy` de Temporal n'a pas de jitter. L'activité calcule donc son propre délai (`RetryBackoff`, exponentiel plafonné, « equal jitter ») et le transmet via `nextRetryDelay`. |
| 5 relances maximum | `MaximumAttempts = 1 + 5`. Au-delà, le jour est enregistré en échec (`vehicle_day_runs`, type, message, tentatives) et le véhicule passe au jour suivant. |

## Trois sources de demandes, les mêmes limites

| Source | Point d'entrée | Mécanisme Temporal | Priorité | Limite par véhicule-jour |
|---|---|---|---|---|
| **Fichier reçu** (temps réel) | `POST /api/vehicles/{id}/files`, ou l'étape de pipeline `ingest.notify-vehicles` | Signal-With-Start `FileReceived` (sans accusé de réception, pour des événements fréquents) | 10 si c'est la journée d'exploitation courante, 5 pour un fichier en retard | Journée courante : **1 traitement par minute**. Autre jour (fichier en retard) : 1 par heure |
| **Demande journalière** (un véhicule, une plage) | `POST /api/vehicles/{id}/requests` | Update-With-Start `Submit` (accusé de réception, validation) | celle de la demande (0 par défaut) | **1 traitement par heure** |
| **Traitement massif** (liste ou tout le parc) | `POST /api/vehicles/requests` (`allKnownVehicles=true` pour tout le parc) | Workflow `FleetRequest` : diffusion durable et à débit régulé (10 véhicules/s par défaut) par Signal-With-Start | celle de la demande | **les mêmes limites** : un jour traité il y a moins d'une heure est reporté à son échéance |

**Les limites ne suppriment jamais une demande.** Une demande reçue pendant l'intervalle minimal est fusionnée avec le jour en attente, et le jour est traité dès l'échéance. Chaque demande est donc suivie d'un traitement qui démarre après elle : « au moins une fois » tient toujours. L'intervalle se compte entre deux **décisions de lancement** du workflow (horloge de Temporal) ; le démarrage effectif de l'activité peut suivre de quelques centaines de millisecondes.

**Journée d'exploitation courante** : calculée avec `OperatingTimeZone` (Europe/Paris par défaut) et `OperatingDayStartHour` (0 = minuit ; 4 = la journée va de 04:00 à 04:00).

**Le temps réel n'attend pas derrière le massif** :
- Dans un véhicule : priorité 10 contre 0, et un jour urgent passe dès la fin du jour en cours.
- Entre véhicules : les activités de la journée courante passent par une **task queue dédiée**, `vehicle-realtime`, avec ses propres places sur les workers. Une file Temporal sert les tâches à peu près dans l'ordre d'arrivée : sans cette séparation, un fichier attendait derrière tout l'arriéré massif.
- La diffusion massive est régulée à 10 véhicules/s, et elle tourne sur la file `control` pour ne pas attendre derrière les traitements qu'elle crée.

Mesuré en conditions réelles (un seul poste de 4 vCPU, ~650 véhicules, traitement massif de 20 000 à 75 000 véhicule-jours en cours) :

| Situation | Délai de traitement d'un fichier temps réel |
|---|---|
| Traitement massif en régime établi | 0,3 à 0,8 s |
| Juste après le lancement massif, diffusion à 10 véhicules/s, workers qui venaient de redémarrer | 0,3 à 3,4 s |
| Même chose à 50 véhicules/s | 11 à 43 s, donc 10/s par défaut |
| Avant la file `vehicle-realtime` dédiée | un fichier attendait derrière l'arriéré de masse |

Une diffusion à 10 véhicules/s atteint 1 500 véhicules en 2 min 30. Pour plus de débit : plus de workers, et un historique plus court (`MaxDaysPerRun`) pour réduire le coût de relecture quand un worker redémarre.

## Fonctionnement

```
API / campagne ──Update-With-Start "Submit"──►  vehicle:veh01  (workflow long, 1 par véhicule)
               ──Signal-With-Start "SubmitRequest"──►   │
                                                        │ file = { jour → priorité, demandes }
                                                        │ boucle : prendre le prochain jour (priorité, puis date)
                                                        ▼
                                          activité vehicle.process-day (1 jour)
                                             ├─ succès → résultat + statut écrits dans une transaction
                                             ├─ transitoire → relance (jitter), 5 fois maximum
                                             └─ métier / applicative → pas de relance
                                                        │ échec final → activité vehicle.record-failure
                                                        ▼
                               inactif 1 h → fin du workflow ; toutes les 500 journées → continue-as-new
```

- **Un jour à la fois par véhicule** : pas d'écritures concurrentes sur un même véhicule, et l'ordre de la file est respecté. Le parallélisme vient des 1 500 véhicules.
- **Priorité** : champ `Priority` de la demande (0 = masse, 10 = urgent), puis la date (les plus récentes d'abord par défaut, `DayOrder`), puis l'ancienneté de la demande. Un jour urgent passe devant le reste d'un gros retraitement dès la fin du jour en cours.
- **Accusé de réception** (`SubmitAck`) : jours ajoutés, fusionnés, remis en file car en cours, doublon de demande, taille de la file.

## Bonnes pratiques Temporal appliquées

- **Identifiant de workflow déterministe** (`vehicle:<id>`), avec `IdConflictPolicy = UseExisting` et `IdReusePolicy = AllowDuplicate` : une demande rejoint le workflow en cours, ou en démarre un nouveau s'il s'est terminé.
- **`*-With-Start` atomique** : pas de course entre « le workflow tourne-t-il ? » et « le démarrer ».
- **`[WorkflowInit]`** : l'état est initialisé avant tout handler (avec Update-With-Start, l'update peut s'exécuter avant `RunAsync`).
- **Validateur d'update** : une demande invalide (dates inversées, plage trop longue, file pleine, mauvais véhicule) est refusée **sans être écrite dans l'historique**.
- **Idempotence à tous les niveaux** : `RequestId` des demandes, écriture d'un jour en transaction (suppression puis insertion), identifiants stables.
- **État borné** : file limitée (`MaxPendingDays`), derniers `RequestId` et dernières erreurs limités, historique borné par un **continue-as-new** (`MaxDaysPerRun`, ou `ContinueAsNewSuggested`) qui reporte la file et les compteurs.
- **Aucun handler en cours au moment de clore un run** : `WaitConditionAsync(() => Workflow.AllHandlersFinished)` avant un continue-as-new ou une fin de workflow.
- **Fin quand inactif** (`IdleTimeout`), puis nouveau run à la demande suivante. Pas de workflow « éternel » à migrer, et le nouveau code s'applique au run suivant.
- **Déterminisme** : tri sur des clés complètes, heure de `Workflow.UtcNow`, aucune I/O dans le workflow. Un test rejoue l'historique (`WorkflowReplayer`).
- **Heartbeat et timeouts** : `StartToClose` de 10 min, heartbeat de 1 min, pour détecter vite un worker tombé.
- **Activité de suivi relancée sans limite** (`record-failure`) : un échec n'est jamais perdu.
- **Task queue dédiée** : un retraitement de masse ne retarde pas l'ingestion des fichiers.
- **Query `Status`** : file du véhicule (prochains jours, jour en cours), compteurs, dernières erreurs, nombre de runs.

## API

```bash
# Une demande (réponse = accusé de réception)
curl -XPOST localhost:5055/api/vehicles/veh01/requests -H 'content-type: application/json' \
  -d '{"from":"2026-01-01","to":"2026-02-01","priority":0,"requestId":"reprocess-2026-09-27"}'

# Fichier reçu (temps réel) : la journée courante au plus 1 fois par minute
curl -XPOST localhost:5055/api/vehicles/veh01/files -H 'content-type: application/json' \
  -d '{"day":"2026-09-27","fileKey":"incoming/veh01/2026-09-27T10-15.csv"}'

# La même demande pour tout le parc (ex. après un changement de configuration) : workflow FleetRequest
curl -XPOST localhost:5055/api/vehicles/requests -H 'content-type: application/json' \
  -d '{"allKnownVehicles":true,"from":"2026-06-01","to":"2026-08-31","requestId":"config-v42"}'
curl localhost:5055/api/vehicles/requests/config-v42                    # progression de la diffusion

# Notification automatique des véhicules à la réception des fichiers (étape de pipeline, sans redéploiement)
curl -XPUT localhost:5055/api/pipelines/file-ingestion -H 'content-type: application/json' -d '{"steps":[
  {"activity":"ingest.fetch"},{"activity":"ingest.validate"},{"activity":"ingest.convert"},
  {"activity":"ingest.store"},{"activity":"ingest.notify-vehicles"}]}'

curl localhost:5055/api/vehicles/veh01                                  # file en cours + compteurs par statut
curl "localhost:5055/api/vehicles/veh01/days?from=2026-01-01&to=2026-01-31"
```

## Tests (`dotnet test --filter Vehicle`)

Sur un vrai serveur Temporal local, avec les vraies activités et un traitement de jour simulé :
- demandes qui se chevauchent → chaque jour traité une fois, doublon de demande ignoré ;
- accusé de réception : ajoutés / fusionnés / remis en file ; jour redemandé pendant son traitement → refait ;
- jour urgent traité avant le reste d'un retraitement ;
- erreur transitoire → relancée, puis succès ; erreur persistante → 6 tentatives, puis échec enregistré, et le véhicule continue ;
- erreur métier et erreur applicative → 1 seule tentative ;
- continue-as-new → file et dédoublonnage conservés ;
- workflow inactif terminé → une demande ultérieure démarre un nouveau run ;
- validateur d'update ; rejeu déterministe de l'historique ;
- **temps réel** : 20 fichiers en 5 s sur la journée courante (intervalle de 2 s) donnent 2 à 4 traitements espacés d'au moins l'intervalle, et le dernier fichier est suivi d'un traitement ;
- **journalier et massif** : un jour redemandé dans l'intervalle est fusionné puis traité à l'échéance, sans bloquer les autres jours ;
- **fichier en retard** : il suit l'intervalle journalier, alors que la journée courante suit l'intervalle temps réel et passe en premier ;
- **journée d'exploitation** : fuseau horaire et heure de début ;
- **diffusion massive** : chaque véhicule atteint une fois, à débit régulé, sans rediffusion si la demande est renvoyée.

Tests unitaires : bornes du backoff et du jitter, classification des erreurs, activité isolée (`ActivityEnvironment`).

Vérifié aussi de bout en bout (API, Postgres, Temporal) :
- avec 30 % d'erreurs transitoires injectées : 4 véhicules × 32 jours avec chevauchements, chaque jour traité une fois, 37 relances, erreur métier sans relance ;
- 20 fichiers ingérés avec l'étape `ingest.notify-vehicles` : 621 véhicules notifiés, 985 véhicule-jours traités une fois chacun ;
- deux fichiers de la journée courante à 1 s d'écart : un traitement, puis un second regroupant les deux fichiers une minute plus tard ;
- traitement massif sur tout le parc : les jours traités depuis moins d'une heure sont reportés à leur échéance, les autres traités immédiatement.

## À adapter

- `MeasurementVehicleDayProcessor` est un traitement de démonstration : remplacez `IVehicleDayProcessor` par le vôtre, en gardant l'idempotence (écriture par véhicule-jour dans une transaction).
- Réglages (`VehicleProcessingSettings`) :
  - délai d'inactivité, taille d'un run, file maximale, ordre des dates ;
  - intervalles temps réel et journalier, fuseau horaire et heure de début de la journée d'exploitation, priorités des fichiers, task queue temps réel ;
  - timeouts et relances.
- `IVehicleRegistry` (« tout le parc ») : la démo prend les véhicules déjà traités ; remplacez-la par votre référentiel de véhicules.
- Correspondance fichier → véhicules dans `ingest.notify-vehicles` : la démo prend l'identifiant du capteur comme identifiant du véhicule.
- Les fuseaux horaires sont lus sur le système du worker : gardez la même base de fuseaux (tzdata) sur tous les workers, pour que le rejeu reste déterministe.
- Pour un traitement d'un jour en plusieurs étapes, utiliser plusieurs activités, ou un workflow enfant si le jour a son propre cycle de vie.
