# Sécurité — Air Quality & Decarbonization Data Platform

## Table des matières

1. [Modèle de menaces](#1-modèle-de-menaces)
2. [OpenShift : SCC et contraintes](#2-openshift--scc-et-contraintes)
3. [RBAC par namespace](#3-rbac-par-namespace)
4. [Gestion des secrets API](#4-gestion-des-secrets-api)
5. [Network Policies](#5-network-policies)
6. [Chiffrement](#6-chiffrement)
7. [Sécurité MinIO](#7-sécurité-minio)
8. [Sécurité TimescaleDB](#8-sécurité-timescaledb)
9. [Sécurité Prefect et MLflow](#9-sécurité-prefect-et-mlflow)
10. [Audit et traçabilité](#10-audit-et-traçabilité)
11. [Checklist de mise en production](#11-checklist-de-mise-en-production)

---

## 1. Modèle de menaces

### Acteurs et surfaces d'attaque

| Surface | Risque | Criticité |
|---|---|---|
| Clés API externes (Airly, CAMS, OpenAQ...) | Fuite → abus de quotas ou facturation | Haute |
| MinIO (object store) | Accès non autorisé aux données brutes/Iceberg | Haute |
| TimescaleDB | Injection SQL, accès aux séries capteurs | Haute |
| Prefect Server (UI/API) | Exécution de flows arbitraires | Haute |
| MLflow | Accès aux modèles, tampering des artefacts | Moyenne |
| PostgreSQL | Backend Prefect + MLflow, accès interne | Moyenne |
| Worker pods | Exfiltration des secrets en mémoire | Moyenne |
| Nessie (catalog) | Modification de métadonnées Iceberg | Moyenne |

### Données sensibles

- **Clés API** : Airly, OpenAQ, PurpleAir, CAMS ADS, Breathe London, AirQo, AirNow
- **Données réglementaires** : inventaires GHG (EDGAR), bilans BEGES entreprises (SIREN + données financières CO₂)
- **Données capteurs IoT** : coordonnées GPS précises, séries temporelles par localisation

---

## 2. OpenShift : SCC et contraintes

### Principe : aucun container root

Tous les pods de la plateforme doivent tourner avec un UID non-root. OpenShift applique ce principe via les **Security Context Constraints (SCC)**.

**SCC recommandée : `restricted-v2`** (défaut OCP 4.11+)
- Pas de root (`runAsNonRoot: true`)
- UID aléatoire dans la plage assignée au namespace
- Pas de capabilities Linux
- Pas d'hostPath volumes
- `seccompProfile: RuntimeDefault`

### Configuration des workloads

```yaml
# À appliquer sur tous les Deployments et Jobs Prefect
spec:
  template:
    spec:
      securityContext:
        runAsNonRoot: true
        runAsUser: 1000          # UID fixe non-root
        fsGroup: 1000
        seccompProfile:
          type: RuntimeDefault

      containers:
      - name: worker
        securityContext:
          allowPrivilegeEscalation: false
          readOnlyRootFilesystem: true
          capabilities:
            drop: ["ALL"]
        volumeMounts:
        - name: tmp
          mountPath: /tmp        # Seul répertoire writable (DuckDB spill)
        - name: duckdb-spill
          mountPath: /tmp/duckdb_spill

      volumes:
      - name: tmp
        emptyDir: {}
      - name: duckdb-spill
        emptyDir:
          sizeLimit: 20Gi
```

### SCC pour TimescaleDB et MinIO

Ces composants nécessitent parfois des privileges supplémentaires :

```bash
# TimescaleDB — nécessite fsGroup pour les PVCs
oc adm policy add-scc-to-serviceaccount anyuid \
  -z timescaledb-sa -n pipeline-storage
# → Préférer une SCC custom plutôt que anyuid en prod

# MinIO ODF — géré par ODF operator, SCC appliquée automatiquement
```

---

## 3. RBAC par namespace

### Principe du moindre privilège

Chaque namespace a son propre ServiceAccount avec uniquement les droits nécessaires.

### `pipeline-cicd`

```yaml
# ServiceAccount Tekton
apiVersion: v1
kind: ServiceAccount
metadata:
  name: tekton-sa
  namespace: pipeline-cicd
---
# Droits : créer des PipelineRuns, lire les secrets CI
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: tekton-role
  namespace: pipeline-cicd
rules:
- apiGroups: ["tekton.dev"]
  resources: ["pipelineruns", "taskruns"]
  verbs: ["create", "get", "list", "watch", "delete"]
- apiGroups: [""]
  resources: ["secrets"]
  verbs: ["get"]                 # lecture seule des secrets CI/CD
- apiGroups: [""]
  resources: ["configmaps"]
  verbs: ["get", "list"]
```

### `pipeline-orchestration`

```yaml
# ServiceAccount Prefect Server
apiVersion: v1
kind: ServiceAccount
metadata:
  name: prefect-server-sa
  namespace: pipeline-orchestration
---
# Droits : créer des Jobs dans pipeline-compute
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: prefect-job-creator
rules:
- apiGroups: ["batch"]
  resources: ["jobs"]
  verbs: ["create", "get", "list", "watch", "delete"]
  resourceNames: []  # limité par namespace via RoleBinding
- apiGroups: [""]
  resources: ["pods", "pods/log"]
  verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: prefect-job-creator-binding
  namespace: pipeline-compute    # Prefect crée des Jobs ICI
subjects:
- kind: ServiceAccount
  name: prefect-server-sa
  namespace: pipeline-orchestration
roleRef:
  kind: ClusterRole
  name: prefect-job-creator
  apiGroup: rbac.authorization.k8s.io
```

### `pipeline-compute` (worker pods)

```yaml
# ServiceAccount des workers Prefect (Jobs éphémères)
apiVersion: v1
kind: ServiceAccount
metadata:
  name: pipeline-worker-sa
  namespace: pipeline-compute
---
# Droits minimaux : aucun accès Kubernetes
# Les workers accèdent à MinIO et TimescaleDB via réseau (pas via k8s API)
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: worker-minimal
  namespace: pipeline-compute
rules:
- apiGroups: [""]
  resources: ["secrets"]
  verbs: ["get"]                 # Lecture des secrets (clés API)
  resourceNames:
  - "airly-api-key"
  - "cams-credentials"
  - "minio-credentials"
  - "timescaledb-credentials"
```

### `pipeline-storage`

```yaml
# ServiceAccount Nessie
apiVersion: v1
kind: ServiceAccount
metadata:
  name: nessie-sa
  namespace: pipeline-storage
---
# Nessie n'a besoin d'aucun accès Kubernetes — uniquement son PVC
```

---

## 4. Gestion des secrets API

### Principe : jamais de secret en clair dans le code ou les images

**Inventaire des secrets :**

| Secret | Namespace | Clé(s) |
|---|---|---|
| `airly-api-key` | pipeline-compute | `AIRLY_API_KEY` |
| `openaq-api-key` | pipeline-compute | `OPENAQ_API_KEY` |
| `purpleair-api-key` | pipeline-compute | `PURPLEAIR_API_KEY` |
| `breathe-london-key` | pipeline-compute | `BL_API_KEY` |
| `airnow-api-key` | pipeline-compute | `AIRNOW_API_KEY` |
| `airqo-api-key` | pipeline-compute | `AIRQO_API_KEY` |
| `cams-credentials` | pipeline-compute | `CAMS_URL`, `CAMS_API_KEY` |
| `minio-credentials` | pipeline-compute | `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY` |
| `timescaledb-credentials` | pipeline-compute | `TSDB_HOST`, `TSDB_USER`, `TSDB_PASSWORD` |
| `postgres-credentials` | pipeline-orchestration | `PG_HOST`, `PG_USER`, `PG_PASSWORD` |
| `mlflow-s3-credentials` | pipeline-orchestration | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` |

### Création des secrets OCP

```bash
# Clés API externes (depuis variables d'environnement locales)
oc create secret generic airly-api-key \
  --from-literal=AIRLY_API_KEY="${AIRLY_API_KEY}" \
  -n pipeline-compute

# CAMS : le .cdsapirc devient un Secret
oc create secret generic cams-credentials \
  --from-literal=CAMS_URL="https://ads.atmosphere.copernicus.eu/api/v2" \
  --from-literal=CAMS_API_KEY="${CAMS_UID}:${CAMS_KEY}" \
  -n pipeline-compute

# MinIO credentials
oc create secret generic minio-credentials \
  --from-literal=MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY}" \
  --from-literal=MINIO_SECRET_KEY="${MINIO_SECRET_KEY}" \
  --from-literal=MINIO_ENDPOINT="http://minio.pipeline-storage.svc:9000" \
  -n pipeline-compute
```

### Injection dans les pods workers

```yaml
# Dans le Job template Prefect (géré par le Kubernetes Work Pool)
env:
- name: AIRLY_API_KEY
  valueFrom:
    secretKeyRef:
      name: airly-api-key
      key: AIRLY_API_KEY
- name: MINIO_ACCESS_KEY
  valueFrom:
    secretKeyRef:
      name: minio-credentials
      key: MINIO_ACCESS_KEY
- name: MINIO_SECRET_KEY
  valueFrom:
    secretKeyRef:
      name: minio-credentials
      key: MINIO_SECRET_KEY
```

### CAMS : fichier .cdsapirc via Secret

```yaml
# Monter .cdsapirc depuis un Secret dans le home du worker
volumes:
- name: cams-config
  secret:
    secretName: cams-credentials
    items:
    - key: CAMS_URL
      path: .cdsapirc
      # Le fichier doit avoir le format :
      # url: <CAMS_URL>
      # key: <CAMS_API_KEY>
volumeMounts:
- name: cams-config
  mountPath: /home/worker
  readOnly: true
```

### Rotation des secrets

```bash
# Rotation d'une clé API sans downtime
oc create secret generic airly-api-key \
  --from-literal=AIRLY_API_KEY="${NEW_KEY}" \
  --dry-run=client -o yaml | oc apply -f -

# Les nouveaux pods créés par Prefect liront la nouvelle valeur
# Les pods en cours d'exécution gardent l'ancienne valeur jusqu'à la fin du Job
```

---

## 5. Network Policies

### Principe : communication inter-namespace explicite uniquement

Par défaut, tous les namespaces sont isolés. Seules les communications nécessaires sont autorisées.

```yaml
# pipeline-compute : autoriser l'accès sortant vers pipeline-storage uniquement
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: worker-egress
  namespace: pipeline-compute
spec:
  podSelector: {}        # Tous les pods du namespace
  policyTypes: ["Egress"]
  egress:
  # MinIO
  - ports:
    - port: 9000
    to:
    - namespaceSelector:
        matchLabels:
          kubernetes.io/metadata.name: pipeline-storage
  # TimescaleDB
  - ports:
    - port: 5432
    to:
    - namespaceSelector:
        matchLabels:
          kubernetes.io/metadata.name: pipeline-storage
  # Nessie (catalog Iceberg)
  - ports:
    - port: 19120
    to:
    - namespaceSelector:
        matchLabels:
          kubernetes.io/metadata.name: pipeline-storage
  # Prefect Server (polling)
  - ports:
    - port: 4200
    to:
    - namespaceSelector:
        matchLabels:
          kubernetes.io/metadata.name: pipeline-orchestration
  # APIs externes (Airly, EEA, CAMS, BEGES...)
  - ports:
    - port: 443
    to:
    - ipBlock:
        cidr: 0.0.0.0/0
        except:
        - 10.0.0.0/8        # Pas d'accès aux IPs internes via HTTPS
        - 172.16.0.0/12
        - 192.168.0.0/16

---
# pipeline-storage : refuser tout ingress non autorisé
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: storage-ingress
  namespace: pipeline-storage
spec:
  podSelector: {}
  policyTypes: ["Ingress"]
  ingress:
  # Uniquement depuis pipeline-compute et pipeline-orchestration
  - from:
    - namespaceSelector:
        matchLabels:
          kubernetes.io/metadata.name: pipeline-compute
    - namespaceSelector:
        matchLabels:
          kubernetes.io/metadata.name: pipeline-orchestration
```

---

## 6. Chiffrement

### En transit (TLS)

| Communication | Protocole | Certificat |
|---|---|---|
| Externe → Prefect UI | HTTPS (Route OCP) | OCP cert-manager / Let's Encrypt |
| Externe → MLflow UI | HTTPS (Route OCP) | OCP cert-manager |
| Worker → MinIO | HTTP interne (mTLS via service mesh optionnel) | Service mesh ou plain HTTP interne |
| Worker → TimescaleDB | TLS PostgreSQL (`sslmode=require`) | Certificat auto-signé OCP |
| Worker → APIs externes | HTTPS | Validé par CA système |

**Recommandation :** activer OpenShift Service Mesh (Istio) pour le mTLS inter-pods automatique, si la posture de sécurité l'exige.

### Au repos

| Stockage | Chiffrement | Mécanisme |
|---|---|---|
| MinIO (ODF) | ✅ AES-256 | ODF chiffre les volumes PVC |
| TimescaleDB | ✅ Transparent Data Encryption | PVC chiffré par ODF |
| PostgreSQL | ✅ Idem | PVC chiffré par ODF |
| Secrets OCP | ✅ etcd encryption | Activé par défaut sur OCP 4.x |

### Clés de chiffrement

Utiliser **OCP Secrets Encryption** avec un KMS externe (HashiCorp Vault ou AWS KMS) pour les environnements de production sensibles :

```bash
# Activation du chiffrement etcd avec Vault KMS
oc edit apiserver cluster
# → encryption.type: aescbc  (ou kms si Vault configuré)
```

---

## 7. Sécurité MinIO

### Buckets et politiques d'accès

```bash
# Créer les buckets avec accès refusé par défaut
mc mb minio/raw minio/iceberg minio/mlflow-artifacts minio/cache

# Politique lecture seule pour les workers (lecture Iceberg)
mc policy set-json - minio/iceberg << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {"AWS": ["arn:aws:iam:::user/pipeline-worker"]},
    "Action": ["s3:GetObject", "s3:ListBucket"],
    "Resource": ["arn:aws:s3:::iceberg", "arn:aws:s3:::iceberg/*"]
  }]
}
EOF

# Politique lecture+écriture pour les workers (ingestion → raw + iceberg)
mc admin policy create minio pipeline-worker-policy - << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject",
                 "s3:ListBucket", "s3:GetBucketLocation"],
      "Resource": [
        "arn:aws:s3:::raw/*", "arn:aws:s3:::raw",
        "arn:aws:s3:::iceberg/*", "arn:aws:s3:::iceberg",
        "arn:aws:s3:::cache/*", "arn:aws:s3:::cache"
      ]
    },
    {
      "Effect": "Allow",
      "Action": ["s3:PutObject"],
      "Resource": ["arn:aws:s3:::mlflow-artifacts/*"]
    }
  ]
}
EOF
```

### Audit MinIO

```bash
# Activer les logs d'audit MinIO (vers stdout → collecté par OCP logging)
mc admin config set minio audit_webhook \
  endpoint="http://log-collector.pipeline-cicd.svc:8080/audit" \
  auth_token="${LOG_COLLECTOR_TOKEN}"
```

---

## 8. Sécurité TimescaleDB

```sql
-- Utilisateurs dédiés par rôle
CREATE USER pipeline_writer PASSWORD '...';  -- Ingestion IoT
CREATE USER pipeline_reader PASSWORD '...';  -- Requêtes ML/dashboard
CREATE USER prefect_backend PASSWORD '...';  -- Backend Prefect (autre DB)

-- Droits minimaux
GRANT CONNECT ON DATABASE pipeline TO pipeline_writer, pipeline_reader;
GRANT USAGE ON SCHEMA public TO pipeline_writer, pipeline_reader;

GRANT INSERT, SELECT ON sensor_readings TO pipeline_writer;
GRANT SELECT ON sensor_readings, sensor_hourly, sensor_daily TO pipeline_reader;

-- Révoquer droits superflus
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
```

### Connexion sécurisée depuis les workers

```python
import os
import psycopg2

def get_tsdb_connection():
    return psycopg2.connect(
        host=os.environ["TSDB_HOST"],
        port=5432,
        dbname="pipeline",
        user=os.environ["TSDB_USER"],
        password=os.environ["TSDB_PASSWORD"],
        sslmode="require",          # TLS obligatoire
        connect_timeout=10,
        application_name="pipeline-worker",
    )
```

---

## 9. Sécurité Prefect et MLflow

### Prefect Server : authentification

```yaml
# Activer l'authentification Prefect via OAuth2 / OIDC OCP
# prefect-server-config ConfigMap
PREFECT_SERVER_ANALYTICS_ENABLED: "false"
PREFECT_API_AUTH_STRING: "${PREFECT_AUTH_SECRET}"   # Injecté depuis Secret
```

### MLflow : protection de l'UI

MLflow n'a pas d'authentification native. Exposer via une Route OCP avec auth proxy :

```yaml
# Route OCP avec annotation OAuth proxy
apiVersion: route.openshift.io/v1
kind: Route
metadata:
  name: mlflow
  namespace: pipeline-orchestration
  annotations:
    haproxy.router.openshift.io/rewrite-target: /
spec:
  to:
    kind: Service
    name: mlflow
  tls:
    termination: edge
    insecureEdgeTerminationPolicy: Redirect
```

```bash
# OAuth proxy sidecar pour protéger MLflow
oc annotate route mlflow \
  kubernetes.io/ingress.class=nginx \
  nginx.ingress.kubernetes.io/auth-url="https://oauth.openshift.io/apis/authentication/v1/users/~"
```

---

## 10. Audit et traçabilité

### Ce qui doit être loggé

| Événement | Source | Destination |
|---|---|---|
| Ingestion réussie/échouée | Worker pod (structuré JSON) | OCP centralized logging |
| Écriture Iceberg | PyIceberg | MLflow + Nessie commit log |
| Accès MinIO (PUT/GET/DELETE) | MinIO audit webhook | OCP logging |
| Accès TimescaleDB | PostgreSQL pg_audit | OCP logging |
| Création/suppression de secrets | OCP audit log | OCP audit |
| Déploiement ArgoCD | ArgoCD events | OCP logging |
| Flow Prefect (start/end/fail) | Prefect events | Prefect DB |

### Format de log structuré (workers)

```python
import logging
import json

class JsonFormatter(logging.Formatter):
    def format(self, record):
        return json.dumps({
            "timestamp": self.formatTime(record),
            "level":     record.levelname,
            "message":   record.getMessage(),
            "source":    record.name,
            "family":    getattr(record, "family", None),
            "run_id":    getattr(record, "run_id", None),
        })

logging.getLogger().addHandler(
    logging.StreamHandler()  # stdout → collecté par OCP
)
logging.getLogger().handlers[0].setFormatter(JsonFormatter())
```

### Iceberg : traçabilité des données via snapshots

```python
# Chaque écriture Iceberg crée un snapshot immutable
# → audit trail natif de toutes les modifications

table = catalog.load_table("decarb.edgar_emissions")

# Lister l'historique des modifications
for snapshot in table.history():
    print(f"{snapshot.timestamp_ms}: {snapshot.snapshot_id} — {snapshot.summary}")

# Time travel : requêter l'état d'une table à une date passée
table.scan(snapshot_id=snapshot.snapshot_id).to_pandas()
```

---

## 11. Checklist de mise en production

### Avant le premier déploiement

- [ ] Tous les secrets créés dans OCP (aucune valeur en dur dans les manifests)
- [ ] SCC `restricted-v2` appliquée à tous les workloads (vérifier avec `oc describe pod`)
- [ ] Network Policies en place et testées (`oc exec` cross-namespace doit échouer)
- [ ] Chiffrement etcd activé sur le cluster OCP
- [ ] MinIO : buckets créés, politiques d'accès appliquées, audit webhook configuré
- [ ] TimescaleDB : users séparés reader/writer, TLS activé, pg_audit configuré
- [ ] MLflow protégé par OAuth proxy (pas exposé en accès public)
- [ ] Prefect Server derrière Route TLS, authentification activée
- [ ] PVCs sur storage class chiffrée (ODF)
- [ ] Rotation des secrets documentée (procédure en < 10 min)
- [ ] Logs centralisés configurés (OCP EFK ou Loki stack)
- [ ] Alertes sécurité configurées (0 ingestion records → alerte)

### Vérifications périodiques

| Fréquence | Action |
|---|---|
| Hebdomadaire | Rotation des clés API actives (si politique de rotation définie) |
| Mensuelle | Revue des accès RBAC (qui a accès à quoi) |
| Trimestrielle | Audit des logs MinIO (accès inattendus ?) |
| Semestrielle | Scan de vulnérabilités des images Docker |
