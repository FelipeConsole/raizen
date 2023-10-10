Teste Raízen
===============



Criar pipeline para extrair dados de tabelas pivotadas de um arquivo .xls e estruturá-los.


Instruções de uso: 
  - clone o repositóro;
  - entre no diretório que foi clonado;
  - execute o comando no terminal:
      sudo docker-compose up -d
  - acesse em seu navegador:
      http://localhost:8080/

Lá podemos executar as DAGs desse repositório.  



Tecnlogias usadas:
  - Airflow
  - Docker


Links que foram úteis no desenvolvimento do teste:
  - [Stack Overflow - How to extract Excel PivotCache into Pandas Data Frame?](https://stackoverflow.com/questions/59330853/how-to-extract-excel-pivotcache-into-pandas-data-frame)

  - [Documentação Apache-Airflow: Building the image](https://airflow.apache.org/docs/docker-stack/build.html#building-the-image)

  - [Documentação openpyxl](https://openpyxl.readthedocs.io/en/stable/index.html)