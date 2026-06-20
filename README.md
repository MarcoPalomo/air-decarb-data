# air-decarb-data

Pipeline de données environnementales — qualité de l'air, pollution, bilan carbone, prédiction.

→ Documentation complète dans [docs/](docs/README.md)

## Structure

```
air-decarb-data/
├── pipeline/          # Code applicatif (familles + sources + schéma)
├── flows/             # Flows Prefect (orchestration)
├── data/edgar/        # Données EDGAR v4.3.2 (locales, Gg, 1970–2012)
├── infra/             # Manifests Tekton + ArgoCD + Dockerfile
├── docs/              # Architecture, Performance, Sécurité, Tests
└── references/        # Méthodologies et docs API tierces (lecture seule)
```

## Démarrage rapide

```bash
pip install -e ".[dev]"
pytest pipeline/tests/ -m "not integration"
```

Voir [docs/README.md](docs/README.md) pour les exemples complets.
