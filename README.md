# Recommandation-de-Films-Jay-Z-Entertainment-spark-kafa-elasticsearch-ML
Jay-Z Entertainment lance un projet innovant de recommandation de films utilisant les données de MovieLens. Ce projet vise à intégrer l'analyse Big Data et l'apprentissage automatique pour améliorer les suggestions de films, en utilisant Apache Spark, Elasticsearch et une API Flask

## Système de Recommandation de Films en Temps Réel

Ce projet propose un système de recommandation de films en temps réel basé sur Apache Spark, Elasticsearch, Kibana et Flask. Le rapport fournit un guide détaillé pour le déploiement et l'utilisation du système, en couvrant chaque étape, de la configuration initiale à la visualisation des recommandations.

### Points Clés du Projet

## 1. Configuration de l'Environnement
   - Installation et configuration d'Apache Spark, Elasticsearch, Kibana et Flask sur une machine locale Windows.

## 2. Acquisition des Données
   - Téléchargement de l'ensemble de données MovieLens.

## 3. Prétraitement des Données avec Apache Spark
   - Utilisation de Spark pour charger et prétraiter les données MovieLens.
   - Création de trois fils pour la consommation de données de films, données utilisateur et données de notation utilisateur.

## 4. Construction du Modèle de Recommandation avec ALS
   - Utilisation du modèle ALS (Alternating Least Squares) pour la recommandation de films.
   - Entraînement, évaluation du modèle, et génération de recommandations pour les utilisateurs.

## 5. Intégration des Données dans Elasticsearch
   - Création d'indices Elasticsearch pour les données de films et d'utilisateurs.
   - Transformation et ingestion des données traitées dans Elasticsearch.

## 6. Visualisation avec Kibana
   - Création de tableaux de bord Kibana basés sur les données Elasticsearch.
   - Visualisations telles que la distribution des évaluations moyennes, les genres de films préférés, et l'activité des utilisateurs au fil du temps.

## 7. Développement de l'API Flask
   - Création d'une API Flask pour recevoir les titres de films des utilisateurs.
   - Recherche des utilisateurs associés au film spécifié et génération de recommandations.

## 8. Conformité au RGPD et Gouvernance des Données
   - Assurer la conformité au RGPD en matière de consentement utilisateur et protection des données personnelles.
   - Mise en place de politiques de gouvernance des données pour garantir qualité, sécurité, et intégrité des données.
   - Développement et maintenance d'un catalogue de données détaillé.

## 9. Catalogue de Données
   - Développement d'un catalogue de données détaillé pour assurer une traçabilité complète des sources de données, des définitions de variables, et des métadonnées associées.
