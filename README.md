# Météo Étude de Cas

Ce projet a pour but de réaliser une **analyse exploratoire des données (EDA)** sur plusieurs fichiers météorologiques, de les transformer, et de les fusionner dans un format commun. Voici un aperçu des principales étapes et fonctionnalités du projet.

<details>
  <summary>Structure</summary>

  - `dags/` : Contient les DAGs Airflow pour orchestrer les tâches.
  - `dashboard/` : Comprend les fichiers pour le tableau de bord de visualisation des données.
  - `data/` : Stocke les fichiers de données bruts et transformés.
  - `diagramme/` : Contient les diagrammes du modèle.
  - `EDA/` : Dossier pour l'analyse exploratoire des données.
  - `scripts/` : Inclut les scripts pour le traitement et la transformation des données.
  - `utils/` : Regroupe les fonctions utilitaires et modules d'assistance.
</details>

## DAGs

Le dossier `dags/` contient trois DAGs (Directed Acyclic Graphs) Airflow pour orchestrer les tâches de traitement des données météorologiques.

<details>
  <summary>meteo_archive_etl.py</summary>

  * **Tâches** :
    + Extraction des données météorologiques pour chaque ville
    + Fusion des données météorologiques
  * **Planification** : @daily
  * **Début** : 2022-06-01
</details>

<details>
  <summary>open_meteo_etl.py</summary>

  * **Tâches** :
    + Extraction des données météorologiques pour chaque ville
    + Fusion des données météorologiques
  * **Planification** : @daily
  * **Début** : 2025-05-30
</details>

<details>
  <summary>open_weather_etl.py</summary>

  * **Tâches** :
    + Extraction des données météorologiques pour chaque ville
    + Fusion des données météorologiques
  * **Planification** : @daily
  * **Début** : 2025-05-30
</details>

## Scripts

Le dossier `scripts/` contient des scripts Python qui sont utilisés pour traiter les données météorologiques. Ces scripts sont appelés par les DAGs pour effectuer des tâches spécifiques telles que l'extraction, la fusion et la transformation des données.

<details>
  <summary>Organisation</summary>

  Le dossier `scripts/` est organisé en sous-dossiers qui correspondent à différents types de données météorologiques :

  * `archive/` : scripts pour traiter les données météorologiques archivées à partir de OpenMeteo
  * `openmeteo/` : scripts pour traiter les données météorologiques en temps réel à partir de OpenMeteo
  * `openweather/` : scripts pour traiter les données météorologiques en temps réel à partir de OpenWeather
</details>

## Upload vers SUPABASE

Les données météorologiques fusionnées sont ensuite uploadées vers une base de données SUPABASE. Cela permet de stocker les données de manière structurée et de les rendre facilement accessibles pour d'autres applications.

Le script `upload_to_supabase.py` dans le dossier `supabase/`  utilise la bibliothèque `supabase` pour se connecter à la base de données et y uploader les données.

## Utilitaires

Le dossier `utils` contient des utilitaires et des fonctions qui peuvent être utilisées dans différents endroits du projet. Il contient le fichier suivant :

* `cities.py` : utilitaire pour la gestion des villes

## Données

Le dossier `data/` contient les données météorologiques qui sont utilisées par les scripts et les DAGs pour traiter et analyser les données.

Ces données sont organisées en sous-dossiers pour refléter les différentes étapes de traitement et les sources de données.

<details>
  <summary>Structure</summary>

  Le dossier `data/` est organisé de la manière suivante :

  * `raw/` : contient les données météorologiques brutes, non traitées
    + `archive/` : données météorologiques archivées
    + `openmeteo/` : données météorologiques de OpenMeteo
    + `openweather/` : données météorologiques de OpenWeather
  * `processed/` : contient les données météorologiques traitées, mais non encore transformées en schéma étoilé
    + `archive/` : données météorologiques archivées traitées
    + `openmeteo/` : données météorologiques de OpenMeteo traitées
    + `openweather/` : données météorologiques de OpenWeather traitées
  * `star_schema/` : contient les fichiers CSV de modélisation des données météorologiques en schéma étoilé
    + `dim_time.csv` : dimension date
    + `dim_city.csv` : dimension ville
    + `dim_country.csv` : dimension pays
    + `dim_condition.csv` : dimension condition météo
    + `fact_weather.csv` : fait météo
  * `cities_data.csv` : données des villes
  * `meteo_data_final.csv` : données météorologiques apres EDA
</details>

## Diagramme de Conception de Données (MCD)

Le dossier `diagramme/` contient une capture de l'écran du Modèle de Conception de Données (MCD) du projet.

Le diagramme de conception de données (MCD) est un modèle de données qui décrit la structure et les relations entre les différentes tables de données. Dans ce cas, le MCD est composé de cinq tables :

* `dim_time` : dimension date
* `dim_city` : dimension ville
* `dim_country` : dimension pays
* `dim_condition` : dimension condition météo
* `fact_weather` : fait météo

Les tables sont liées entre elles par des clés étrangères. Voici les relations entre les tables :

* `dim_time` est liée à `fact_weather` par la clé étrangère `time_id`
* `dim_city` est liée à `fact_weather` par la clé étrangère `city_id`
* `dim_country` est liée à `dim_city` par la clé étrangère `country_id`
* `dim_condition` est liée à `fact_weather` par la clé étrangère `condition_id`

## Tableau de Bord

Le dossier `dashboard/` contient une fichier `pdf` du tableau de bord de visualisation des données météorologiques.

Cette capture montre un exemple de la manière dont les données météorologiques sont visualisées et présentées dans le tableau de bord.

Notez que cette capture est statique et ne reflète pas l'état actuel du tableau de bord.

## Analyse Exploratoire de Données (EDA)

Le dossier `EDA` contient des analyses exploratoires de données qui ont été effectuées sur les données météorologiques.

Ces analyses ont pour but de comprendre la structure et les caractéristiques des données, de détecter les anomalies et les tendances, et de préparer les données pour les analyses ultérieures.

Il contient les fichiers suivants :

* `eda_meteo_complet.ipynb` : notebook Jupyter qui contient les analyses exploratoires de données

