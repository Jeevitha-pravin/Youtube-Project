# Youtube-Project
Youtube Data Harvesting and Warehousing

# Introduction
YouTube Data Harvesting and Warehousing is a project aimed at developing a user-friendly Streamlit application that leverages the power of the Google API to extract valuable information from YouTube channels. The extracted data is then stored in a MongoDB database, subsequently migrated to a SQL data warehouse, and made accessible for analysis and exploration within the Streamlit app.

# FEATURES

* Retrieve data from the YouTube API, including channel information, playlists, videos, and comments.
* Store the retrieved data in a MongoDB database.
* Migrate the data to a SQL data warehouse.
* Analyze and visualize data using Streamlit.
* Perform queries on the SQL data warehouse.
* Gain insights into channel performance, video metrics, and more.

# Retrieving data from the YouTube API

The project utilizes the Google API to retrieve comprehensive data from YouTube channels. The data includes information on channels, playlists, videos, and comments. By interacting with the Google API, we collect the data and merge it into a JSON file.

# Storing data in MongoDB

The retrieved data is stored in a MongoDB database based on user authorization. If the data already exists in the database, it can be overwritten with user consent. This storage process ensures efficient data management and preservation, allowing for seamless handling of the collected data.

# Migrating data to a SQL data warehouse

The application allows users to migrate data from MongoDB to a SQL data warehouse. Users can choose which channel's data to migrate. To ensure compatibility with a structured format, the data is cleansed using the powerful pandas library. Following data cleaning, the information is segregated into separate tables, including channels, playlists, videos, and comments, utilizing SQL queries.

# Data Analysis

The project provides comprehensive data analysis capabilities using Streamlit. users can create interactive to gain insights from the collected data.

* Channel Analysis: Channel analysis includes insights on playlists, videos, subscribers, views, likes, comments, and durations. Gain a deep understanding of the channel's performance and audience engagement through detailed summaries.

* Video Analysis: Video analysis focuses on views, likes, comments, and durations, enabling both an overall channel and specific channel perspectives. Leverage visual representations and metrics to extract valuable insights from individual videos.

The Streamlit app provides an intuitive interface to interact with data visually.

With the combined capabilities of Streamlit, the Data Analysis section empowers users to uncover valuable insights and make data-driven decisions.
