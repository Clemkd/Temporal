# Traitement par véhicule sur une plage de temps

Implémentation d'un workflow Temporal par véhicule qui traite des plages de jours demandées à tout moment.
Code : `src/TemporalPoc.Core/Vehicles/`. API : `src/TemporalPoc.Api/Endpoints/VehicleEndpoints.cs`.
Tests : `tests/TemporalPoc.Tests/Vehicle*.cs`.

## Exigences et réponses

| Exigence | Implémentation |
|---|---|
| Jusqu'à 1 500 véhicules | Un workflow par véhicule (`vehicle:<id>`), sur une task queue dédiée `vehicle-processing`. Un workflow qui attend ne consomme presque rien. |
| Demandes à tout moment, sur des plages différentes ou qui se croisent | Chaque demande est ajoutée à la **file de jours** du véhicule : Update-With-Start (avec accusé de réception) ou Signal-With-Start (envoi en masse). Le workflow est démarré s'il ne tourne pas. |
| Traiter chaque jour au moins une fois | Chaque jour demandé entre dans la file et y reste jusqu'à son traitement, même à travers un continue-as-new. Un jour redemandé **pendant** son traitement est refait ensuite. Après une panne, Temporal reprend exactement là où c'était. |
| Pas de doublon pour un jour demandé plusieurs fois et pas encore traité | La file est un ensemble de **jours** (clé = date) et non de demandes. Un jour déjà en attente est fusionné : une seule entrée, la priorité la plus haute, la liste des demandes. Une demande renvoyée avec le même `RequestId` est ignorée. |
| Relance des erreurs transitoires | L'activité classe les erreurs (`FailureClassifier`) : réseau, timeout, base indisponible → `ApplicationFailure` relançable. |
| Pas de relance des erreurs métier ou applicatives | `VehicleBusinessException` → type `BusinessError`, non relançable. Toute autre exception (bug) → `ApplicationError`, non relançable. `NonRetryableErrorTypes` dans la RetryPolicy en plus, par sécurité. |
| Relance exponentielle avec jitter | La `RetryPolicy` de Temporal n'a pas de jitter. L'activité calcule donc son propre délai (`RetryBackoff`, exponentiel plafonné, « equal jitter ») et le transmet via `nextRetryDelay`. |
| 5 relances maximum | `MaximumAttempts = 1 + 5`. Au-delà, le jour est enregistré en échec (`vehicle_day_runs`, type, message, tentatives) et le véhicule passe au jour suivant. |

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

# La même demande pour une liste de véhicules (ex. après un changement de configuration)
curl -XPOST localhost:5055/api/vehicles/requests -H 'content-type: application/json' \
  -d '{"vehicleIds":["veh01","veh02"],"from":"2026-06-01","to":"2026-08-31","requestId":"config-v42"}'

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
- validateur d'update ; rejeu déterministe de l'historique.

Tests unitaires : bornes du backoff et du jitter, classification des erreurs, activité isolée (`ActivityEnvironment`).

Vérifié aussi de bout en bout (API, Postgres, Temporal) avec 30 % d'erreurs transitoires injectées : 4 véhicules × 32 jours avec chevauchements, chaque jour traité une fois, 37 relances, erreur métier sans relance.

## À adapter

- `MeasurementVehicleDayProcessor` est un traitement de démonstration : remplacez `IVehicleDayProcessor` par le vôtre, en gardant l'idempotence (écriture par véhicule-jour dans une transaction).
- Réglages (`VehicleProcessingSettings`) : délai d'inactivité, taille d'un run, file maximale, ordre des dates, timeouts, relances.
- Pour un traitement d'un jour en plusieurs étapes, utiliser plusieurs activités, ou un workflow enfant si le jour a son propre cycle de vie.
