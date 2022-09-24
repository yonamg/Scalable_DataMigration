# Scalable Data-Migration
This project shows about ELT Pipeline that refers to the process of extracting data from source systems, loading it into a Data Warehouse environment, and then transforming it using in-database operations like SQL and/or Postgresql.
### Architectural Design of the Project
![alt text](https://github.com/yonamg/Scalable_DataMigration/blob/screen-shot/screenshoot/Arch_Diag.png?raw=true)
A dockerized Extract, Load, Transform (ELT) pipeline with PostgreSQL, Airflow, DBT, and a Redash.

### Built With

Tech Stack used in this project includes:
* [![Docker][Docker.com]][Docker-url]
* [![Postgres][Postgresql.com]][Postgresql-url]
* [![Airflow][Airflow.com]][Airflow-url]
* [![DBT][DBT.com]][DBT-url]
* [![Redash][Redash.com]][Redash-url]

<!-- GETTING STARTED -->
## Getting Started
### Prerequisites
Make sure you have docker installed on local machine.
-   Docker
-   Docker Compose
-   Python3
-   Pip3

## Contributor
* Yonas Moges

### License
[MIT](https://choosealicense.com/licenses/mit/)
<!-- MARKDOWN LINKS & IMAGES -->
[Postgresql.com]: https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white
[Postgresql-url]: https://www.postgresql.org/
[Airflow.com]: https://img.shields.io/badge/Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white
[Airflow-url]: https://airflow.apache.org/
[Docker.com]: https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white
[Docker-url]: https://www.docker.com/
[DBT.com]: https://img.shields.io/badge/DBT-FF694B?style=for-the-badge&logo=dbt&logoColor=white
[DBT-url]: https://docs.getdbt.com/
[Redash.com]: https://img.shields.io/badge/Redash-ef816b?style=for-the-badge&logo=data:image/svg;base64,PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0iVVRGLTgiPz4KPHN2ZyB3aWR0aD0iMzhweCIgaGVpZ2h0PSIzNXB4IiB2aWV3Qm94PSIwIDAgMzggMzUiIHZlcnNpb249IjEuMSIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIiB4bWxuczp4bGluaz0iaHR0cDovL3d3dy53My5vcmcvMTk5OS94bGluayI+CiAgICA8IS0tIEdlbmVyYXRvcjogU2tldGNoIDQ5LjEgKDUxMTQ3KSAtIGh0dHA6Ly93d3cuYm9oZW1pYW5jb2RpbmcuY29tL3NrZXRjaCAtLT4KICAgIDx0aXRsZT5yZWRhc2gtbG9nbzwvdGl0bGU+CiAgICA8ZGVzYz5DcmVhdGVkIHdpdGggU2tldGNoLjwvZGVzYz4KICAgIDxkZWZzPjwvZGVmcz4KICAgIDxnIGlkPSJyZWRhc2gtbG9nbyIgc3Ryb2tlPSJub25lIiBzdHJva2Utd2lkdGg9IjEiIGZpbGw9Im5vbmUiIGZpbGwtcnVsZT0iZXZlbm9kZCI+CiAgICAgICAgPGcgaWQ9Ikdyb3VwLTUiIHRyYW5zZm9ybT0idHJhbnNsYXRlKDYuMDAwMDAwLCAxLjAwMDAwMCkiIGZpbGwtcnVsZT0ibm9uemVybyI+CiAgICAgICAgICAgIDxwYXRoIGQ9Ik0xMiwyNS4zODQ2MTU0IEMyMC43MjYzMzg3LDIwLjIxMTQxNjQgMjQuODI2NTY4NiwxOC4yMjA2MDE0IDI0LjMwMDY4OTcsMTkuNDEyMTcwNCBDMjMuNzc0ODEwOCwyMC42MDM3Mzk0IDE5LjY3NDU4MDksMjUuMTMzMDE2IDEyLDMzIEwxMiwyNS4zODQ2MTU0IFoiIGlkPSJTaGFwZSIgZmlsbD0iI0ZGNzk2NCI+PC9wYXRoPgogICAgICAgICAgICA8cGF0aCBkPSJNMjYsMTMgQzI2LDIwLjE4NjQyMzcgMjAuMTg1MDU4NCwyNiAxMywyNiBDNS44MTQ5NDE1NSwyNiAwLDIwLjE3MjQ0NyAwLDEzIEMwLjAwMDI0MDQ1NTc1Niw1LjgyNzU1Mjk3IDUuODE1MTgyMDEsMCAxMywwIEMyMC4xODQ4MTgsMCAyNiw1LjgyNzU1Mjk3IDI2LDEzIFoiIGlkPSJTaGFwZSIgZmlsbD0iI0ZGNzk2NCI+PC9wYXRoPgogICAgICAgICAgICA8cGF0aCBkPSJNNC44NDA4OTkxLDE0LjcyNjM1MjggTDYuNzYwODg3NzUsMTQuNzI2MzUyOCBDNy4yMjQ2OTkxNCwxNC43Mjc2ODEyIDcuNjAwMzMxMDIsMTUuMDcwNDIxOCA3LjYwMTc4Njg1LDE1LjQ5MzYyMDggTDcuNjAxNzg2ODUsMTcuMjMyNzMyMSBDNy42MDAzMzEwMiwxNy42NTU5MzEgNy4yMjQ2OTkxNCwxNy45OTg2NzE2IDYuNzYwODg3NzUsMTggTDQuODQwODk5MSwxOCBDNC4zNzcwODc3MSwxNy45OTg2NzE2IDQuMDAxNDU1ODMsMTcuNjU1OTMxIDQsMTcuMjMyNzMyMSBMNCwxNS40OTM2MjA4IEM0LjAwMTMyNDIsMTUuMDcwMzcxOSA0LjM3NzAzMjk2LDE0LjcyNzU2MTEgNC44NDA4OTkxLDE0LjcyNjM1MjggWiBNOS4yODM1ODUwNSwxMC44OTAwMTMyIEwxMS4xODk1OTA5LDEwLjg5MDAxMzIgQzExLjY1MzQwMjMsMTAuODkxMzQxNiAxMi4wMjkwMzQxLDExLjIzNDA4MjIgMTIuMDMwNDksMTEuNjU3MjgxMSBMMTIuMDMwNDksMTcuMjMyNzMyMSBDMTIuMDI5MDM0MSwxNy42NTU5MzEgMTEuNjUzNDAyMywxNy45OTg2NzE2IDExLjE4OTU5MDksMTggTDkuMjgzNTg1MDUsMTggQzguODE5NzczNjcsMTcuOTk4NjcxNiA4LjQ0NDE0MTc5LDE3LjY1NTkzMSA4LjQ0MjY4NTk1LDE3LjIzMjczMjEgTDguNDQyNjg1OTUsMTEuNjU3MjgxMSBDOC40NDQwMTAxNiwxMS4yMzQwMzIyIDguODE5NzE4OTEsMTAuODkxMjIxNSA5LjI4MzU4NTA1LDEwLjg5MDAxMzIgWiBNMTMuNzEyMjg4MiwxMyBMMTUuNjMyMjc2OCwxMyBDMTYuMDk2MDg4MiwxMy4wMDEzMjg0IDE2LjQ3MTcyMDEsMTMuMzQ0MDY5IDE2LjQ3MzE3NTksMTMuNzY3MjY3OSBMMTYuNDczMTc1OSwxNy4yMzI3MzIxIEMxNi40NzE3MjAxLDE3LjY1NTkzMSAxNi4wOTYwODgyLDE3Ljk5ODY3MTYgMTUuNjMyMjc2OCwxOCBMMTMuNzEyMjg4MiwxOCBDMTMuMjQ4NDc2OCwxNy45OTg2NzE2IDEyLjg3Mjg0NDksMTcuNjU1OTMxIDEyLjg3MTM4OTEsMTcuMjMyNzMyMSBMMTIuODcxMzg5MSwxMy43NjcyNjc5IEMxMi44NzI3MTMzLDEzLjM0NDAxOSAxMy4yNDg0MjIsMTMuMDAxMjA4MyAxMy43MTIyODgyLDEzIFogTTE4LjI1MzA5NTEsOCBMMjAuMTU5MTAwOSw4IEMyMC42MjI5MTIzLDguMDAxMzI4MzYgMjAuOTk4NTQ0Miw4LjM0NDA2OSAyMSw4Ljc2NzI2NzkzIEwyMSwxNy4yMzI3MzIxIEMyMC45OTg1NDQyLDE3LjY1NTkzMSAyMC42MjI5MTIzLDE3Ljk5ODY3MTYgMjAuMTU5MTAwOSwxOCBMMTguMjUzMDk1MSwxOCBDMTcuNzg5MjgzNywxNy45OTg2NzE2IDE3LjQxMzY1MTgsMTcuNjU1OTMxIDE3LjQxMjE5NiwxNy4yMzI3MzIxIEwxNy40MTIxOTYsOC43NjcyNjc5MyBDMTcuNDEzNTIwMiw4LjM0NDAxOTA0IDE3Ljc4OTIyODksOC4wMDEyMDgyNSAxOC4yNTMwOTUxLDggWiIgaWQ9IlNoYXBlIiBmaWxsPSIjRkZGRkZGIj48L3BhdGg+CiAgICAgICAgPC9nPgogICAgPC9nPgo8L3N2Zz4=&logoColor=white
[Redash-url]: https://redash.io/
