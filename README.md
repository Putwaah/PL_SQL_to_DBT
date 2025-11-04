# PL/SQL to DBT Migration Project

Un projet de migration et de transformation du code PL/SQL Oracle vers dbt (data build tool).

## 📋 Table des matières

- [À propos du projet](#à-propos-du-projet)
- [Prérequis](#prérequis)
- [Installation](#installation)
- [Structure du projet](#structure-du-projet)
- [Guide de migration](#guide-de-migration)
- [Utilisation](#utilisation)
- [Bonnes pratiques](#bonnes-pratiques)
- [Contribution](#contribution)
- [Ressources](#ressources)

## 🎯 À propos du projet

Ce projet fournit une méthodologie et des outils pour migrer des transformations de données écrites en PL/SQL Oracle vers dbt. L'objectif est de transformer des packages PL/SQL en models DBT pour intégrer la donnée Snowflake.

### Objectifs principaux

- **Modularité** : Décomposer les scripts monolithiques en modèles réutilisables
- **Documentation** : Générer automatiquement la documentation du lineage des données


## 📦 Prérequis

### Environnement requis

- **Python** : 3.8 ou supérieur
- **dbt-snowflake** : 1.5 ou supérieur
- **Git** : Pour le versioning
- **Accès base de données** : Credentials Oracle source et cible

### Connaissance recommandées

- SQL et PL/SQL
- Concepts de base de dbt
- Git et versioning de code
- Modélisation de données

## 🚀 Installation

### 1. Cloner le repository

```bash
git clone https://github.com/Putwaah/PL_SQL_to_DBT.git
cd PL_SQL_to_DBT
```

### 2. Créer un environnement virtuel Python

```bash
python -m venv venv
source venv/bin/activate  # Sur Windows: venv\Scripts\activate
```

### 3. Installer dbt et les dépendances

```bash
pip install dbt-snowflake
# Ou si vous migrez vers une autre plateforme :
# pip install dbt-bigquery
# pip install dbt-redshift
```

### 4. Configurer dbt

```bash
dbt init my_project
cd my_project
```

Configurer votre fichier `profiles.yml` avec vos credentials :

```yaml
my_project:
  target: dev
  outputs:
    dev:
      type: oracle
      host: your-oracle-host
      port: 1521
      user: your-username
      password: your-password
      database: your-database
      schema: your-schema
      threads: 4
```

## 📁 Structure du projet

```bash
PL_SQL_to_DBT/
├── data/                 
│    └── PKG_PL/SQL.sql
├── macros/                 
│    └── cte_utils.py
├── parsing/               
│    └── block_extraction.py
├── pipeline/                  
│    └── normalization_dbz.py
├── runner/
│    └── process.py
│    └── rules.py
├── transforms/
│    └── joins.py
│    └── pivot.py
│    └── pkg_functions.py
│    └── sys_call.py
│    └── table_ref.py
├── utils/
│    └── navigation_sql.py
│    └── str_utils.py          
├── const_regex.py
└── main.py
```

## 🔄 Guide de migration

### Étape 1 : Analyse du code PL/SQL existant

1. **Inventaire** : Lister toutes les procédures stockées, fonctions et scripts
2. **Dépendances** : Identifier les dépendances entre les objets
3. **Complexité** : Évaluer la complexité de chaque transformation


### Étape 2 : Extraction du code

Convertir le code PL/SQL en SQL pur :

```sql
-- PL/SQL original
CREATE OR REPLACE PROCEDURE calculate_customer_ltv AS
BEGIN
  [...]
  TRUNCATE TABLE customer_ltv;
  
  INSERT INTO customer_ltv
  SELECT 
    customer_id,
    SUM(order_total) as lifetime_value
  FROM orders
  GROUP BY customer_id;
  
  COMMIT;
END;
```

En dbt, cela devient :

```sql
-- models/marts/fct_customer_ltv.sql
{{ config(
    materialized='table',
    transient=true,
    alias='customer_ltv'
) }}
SELECT 
    customer_id,
    SUM(order_total) as lifetime_value
FROM {{ ref('stg_orders') }}
GROUP BY customer_id
```

### Étape 3 : Ajout de tests

```yaml
# models/staging/schema.yml
version: 2

models:
  - name: stg_orders
    description: "Orders data from source system"
    columns:
      - name: order_id
        description: "Unique identifier for orders"
        tests:
          - unique
          - not_null
      
      - name: customer_id
        description: "Foreign key to customers"
        tests:
          - not_null
          - relationships:
              to: ref('stg_customers')
              field: customer_id
      
      - name: order_total
        description: "Total order amount"
        tests:
          - not_null
          - positive_value
```

### Étape 4 : Documentation

```yaml
# models/marts/schema.yml
version: 2

models:
  - name: dim_customers
    description: "Customer dimension with aggregated metrics"
    columns:
      - name: customer_id
        description: "Unique customer identifier"
        
      - name: customer_lifetime_days
        description: "Number of days between first and last order"
        meta:
          business_owner: "Marketing Team"
          calculation: "DATEDIFF between first_order_date and last_order_date"
```

## 🎮 Utilisation

### Commandes dbt essentielles

```bash
# Compiler les modèles (sans exécution)
dbt compile

# Exécuter tous les modèles
dbt run

# Exécuter un modèle spécifique
dbt run --select dim_customers

# Exécuter les modèles en aval d'un modèle
dbt run --select stg_orders+

# Exécuter tous les tests
dbt test

# Tester un modèle spécifique
dbt test --select dim_customers

# Générer et servir la documentation
dbt docs generate
dbt docs serve

# Créer un snapshot (SCD Type 2)
dbt snapshot
```

### Workflow typique

```bash
# 1. Développement
dbt run --select +my_new_model  # Exécuter le modèle et ses dépendances
dbt test --select my_new_model  # Tester le modèle

# 2. Validation
dbt build  # Run + Test en une commande

# 3. Documentation
dbt docs generate

# 4. Déploiement (via CI/CD)
dbt run --target prod
dbt test --target prod
```

## 🌟 Bonnes pratiques

### Organisation du code

- **Modularité** : Un modèle = une instruction INSERT

### Performance

- Utiliser la matérialisation appropriée :
  - `view` : Pour les transformations légères
  - `table` : Pour les agrégations lourdes
  - `incremental` : Pour les tables volumineuses avec append
  - `ephemeral` : Pour les CTE réutilisables
  - `transient` : Pour les tables temporaires

### Tests

- **Tests génériques** : `unique`, `not_null`, `accepted_values`, `relationships`
- **Tests personnalisés** : Dans le dossier `dbt_project/tests/`

```yaml
models:
  - name: fct_orders
    tests:
      - dbt_utils.expression_is_true:
          expression: "order_total >= 0"
          config:
            severity: error
            error_if: ">100"  # Échec si plus de 100 violations
```

## 📚 Ressources

### Documentation officielle

- [dbt Documentation](https://docs.getdbt.com/)
- [dbt Best Practices](https://docs.getdbt.com/guides/best-practices)

### Adaptateurs dbt pour bases de données

- [dbt-oracle](https://github.com/oracle/dbt-oracle)
- [dbt-snowflake](https://github.com/dbt-labs/dbt-snowflake)

### Packages dbt utiles

- [dbt-utils](https://github.com/dbt-labs/dbt-utils) : Macros et tests utilitaires
- [dbt-audit-helper](https://github.com/dbt-labs/dbt-audit-helper) : Comparaison de résultats
- [dbt-expectations](https://github.com/calogica/dbt-expectations) : Tests avancés
