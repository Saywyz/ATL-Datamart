# ATL-Datamart Version Python
==============================

<p><small>Forked from <a target="_blank" href="https://github.com/Noobzik/ATL-Datamart"></a></small></p>
<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>


### Gérer l'infrastructure
Vous n'avez pas besoin d'installer l'ensemble de l'architecture sur votre PC. L'intégralité de l'architecture (à l'exception des dépendances de développement) est gérée par le fichier de configuration `docker-compose.yml`.

* Pour lancer l'infrastructure
  ```sh
  docker compose up
  ```
* Pour stopper l'infrastructure
  ```sh
  docker compose down
  ```

**Remarque Linux** :  
- En cas de problème avec le daemon de Docker, c'est parce que vous avez deux Docker installés sur votre machine (Docker Desktop et Docker Engine via CLI). Seule la version Engine sera active, et donc uniquement accessible via `sudo`. Si vous avez besoin de gérer visuellement les conteneurs, je vous invite à utiliser [Portainer](https://docs.portainer.io/start/install-ce/server).

### Description détaillée du sujet

* Pour le TP 1 :  

  * **Approche réalisée** : Implémentation du script Python `src/data/grab_parquet.py` pour télécharger les données des Taxis Jaunes (Jan-Août 2023) et les uploader dans le bucket Minio `nybuck` via la librairie `minio` et l'API S3.
  
* Pour le TP 2 : 

     * **Approche réalisée** : Modification de `src/data/dump_to_sql.py` pour lire les fichiers Parquet (échantillonnés à 1000 lignes pour la performance), nettoyage des colonnes, et insertion optimisée (`chunksize`, `multi`) dans la base PostgreSQL `nyc_warehouse` (Table `nyc_raw`).
     
* Pour le TP 3 :  

  * **Approche réalisée** : Création d'un modèle en flocon (Snowflake) dans `models/creation.sql` (Dimensions : Date, Time, Vendor, Payment, Location ; Fait : Trips). Utilisation de `postgres_fdw` dans `models/insertion.sql` pour transférer et transformer les données du Data Warehouse vers le Data Mart (`nyc_datamart`).
       
* Pour le TP 4 :  

  * **Approche réalisée** : Connexion de Power BI au Data Mart via PostgreSQL (port 15435) pour créer un dashboard interactif (Heatmap, KPIs financiers, répartition temporelle).
  
* Pour le TP 5 :  

  * **Approche réalisée** : Création du DAG `monthly_nyc_data_grab` dans Airflow. Le DAG s'exécute le 1er de chaque mois, calcule le mois précédent, télécharge les données officielles NYC TLC, et les archive sur Minio. Inclusion d'une sécurité pour gérer les dates futures lors des tests manuels.


--------

Project Organization
------------
    ├── airflow
    │   ├── config       <- Configuration files related to the Airflow Instance
    │   ├── dags         <- Folder that contains all the dags
    │   ├── logs         <- Contains the logs of the previously dags run
    │   └── plugins      <- Should be empty : Contains all needed plugins to make the dag work
    ├── LICENSE
    ├── Makefile           <- Makefile with commands like `make data` or `make train`
    ├── README.md          <- The top-level README for developers using this project.
    ├── data
    │   ├── external       <- Data from third party sources.
    │   ├── interim        <- Intermediate data that has been transformed.
    │   ├── processed      <- The final, canonical data sets for modeling.
    │   └── raw            <- The original, immutable data dump.
    │
    ├── docs               <- A default Sphinx project; see sphinx-doc.org for details
    │
    ├── models             <- Trained and serialized models, model predictions, or model summaries
    │
    ├── notebooks          <- Jupyter notebooks. Naming convention is a number (for ordering),
    │                         the creator's initials, and a short `-` delimited description, e.g.
    │                         `1.0-jqp-initial-data-exploration`.
    │
    ├── references         <- Data dictionaries, manuals, and all other explanatory materials.
    │
    ├── reports            <- Generated analysis as HTML, PDF, LaTeX, etc.
    │   └── figures        <- Generated graphics and figures to be used in reporting
    │
    ├── requirements.txt   <- The requirements file for reproducing the analysis environment, e.g.
    │                         generated with `pip freeze > requirements.txt`
    │
    ├── setup.py           <- makes project pip installable (pip install -e .) so src can be imported
    ├── src                <- Source code for use in this project.
    │   ├── __init__.py    <- Makes src a Python module
    │   │
    │   ├── data           <- Scripts to download or generate data
    │   │   └── make_dataset.py
    │   │
    │   ├── features       <- Scripts to turn raw data into features for modeling
    │   │   └── build_features.py
    │   │
    │   ├── models         <- Scripts to train models and then use trained models to make
    │   │   │                 predictions
    │   │   ├── predict_model.py
    │   │   └── train_model.py
    │   │
    │   └── visualization  <- Scripts to create exploratory and results oriented visualizations
    │       └── visualize.py
    │
    └── tox.ini            <- tox file with settings for running tox; see tox.readthedocs.io


--------
