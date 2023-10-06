# Streamlining-Podcast-Data-Processing-with-Apache-Airflow
Apache Airflow powered podcast data pipeline autonomously extracts metadata from XML sources, establishing a high-performance SQLite database for efficient data management. Advanced speech recognition via 'vosk' enables seamless audio transcription, enhancing accessibility and data analysis or querying within the database.

Implemented an Apache Airflow-powered data pipeline for financial podcasts, enhancing efficiency. It autonomously extracts metadata from XML sources, ensuring seamless data retrieval while adhering to Airflow's Directed Acyclic Graph (DAG) structure. A high-performance SQLite database is established for efficient metadata management, with expertly executed data loading processes. Additionally, advanced speech recognition with "vosk" automatically transcribes downloaded audio, enabling robust data analysis and querying within the SQLite database. This streamlined pipeline enhances accessibility to any podcast content while maintaining data integrity and facilitating seamless speech-to-text integration.

**Task 1: Data Extraction and Parsing:**
* Employed Apache Airflow to automate the retrieval of podcast metadata XML.
* Executed parsing operations to extract relevant information.

**Task 2: Efficient Database Management:**
* Leveraged Apache Airflow's capabilities to establish a high-performance SQLite database.
* Ensured optimized organization and management of podcast metadata within the database.

**Task 3: Streamlined Data Loading:**
* Executed data loading processes using Apache Airflow's orchestrated workflows.
* Methodically populated the SQLite database with meticulously curated podcast metadata.

**Task 4: Seamless Audio Retrieval:**
* Utilized Apache Airflow in conjunction with Python's requests library for efficient podcast audio file downloads.
* Ensured streamlined and automated retrieval of actual podcast audio content.

**Task 5: Advanced Speech-to-Text Integration:**
* Incorporated advanced speech recognition technology, "vosk" to transcribe downloaded audio content automatically.
* Stored transcribed data within the SQLite database for further analysis and querying.

**Output Screenhots :**

Airflow DAG :

![ALT text](https://github.com/Pralhad789/Streamlining-Podcast-Data-Processing-with-Apache-Airflow/blob/main/Airflow_DAG.png)
