# ADR 001 — Pourquoi MinIO pour le Data Lake ?

**Date** : 2026-06-09
**Statut** : Accepté

## Contexte

Le pipeline collecte des données météo depuis l'API Open-Meteo pour 4 villes françaises.
Il nous faut un système de stockage pour les couches Bronze (données brutes) et Silver (données transformées)
du Data Lake Medallion.

## Options considérées

| Option | Avantages | Inconvénients |
|--------|-----------|---------------|
| **MinIO** | S3-compatible, local, Docker, gratuit | Nécessite Docker |
| **Amazon S3** | Fully managed, scalable | Coût, nécessite un compte AWS |
| **Fichiers locaux** | Simple | Pas de découplage, difficile à scaler |
| **HDFS** | Standard Big Data | Trop lourd pour ce projet |

## Décision

Nous utilisons **MinIO** pour les raisons suivantes :

1. **Compatibilité S3** — le même code Python (`boto3`) fonctionne avec MinIO en local
   et avec Amazon S3 en production. Migration sans changement de code.
2. **Isolation** — les données sont découplées du système de fichiers local.
3. **Portabilité** — un `docker run` suffit, fonctionne sur toute machine.
4. **Standard industrie** — les data engineers travaillent avec des APIs S3-compatibles
   (AWS S3, GCS, Azure Blob via compatibilité S3). MinIO prépare à cet environnement.

## Conséquences

- Docker est requis pour faire tourner MinIO.
- En production, remplacer `MINIO_ENDPOINT` par une URL S3/GCS suffit — aucun autre changement.
