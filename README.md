# [Engenharia-de-Dados-Short-Track](https://github.com/AhirtonLopes/teste_eng_dados#teste---engenharia-de-dados-short-track)
Essa é minha tentativa de solução para esse teste técnico

### Programas utilizados:
* [Google Cloud Sql](https://cloud.google.com/sql?hl=pt-br)
* [SqlServer](https://www.microsoft.com/pt-br/sql-server)
* [Dbeaver](https://dbeaver.io/download/)
* [Google Colab](https://colab.research.google.com)
* [Pandas](https://pandas.pydata.org)

## Escrever uma aplicação para calcular o ganho total da empresa.
* Para Fazer isso eu criei um banco SQL Server no Cloud SQL e configurei uma conexão para ele autorizar o ip público da minha máquina, para eu conseguir acessar o banco utilizando o [Dbeaver](https://dbeaver.io/download/) (Eu poderia utilizar o BigQuery, mas optei pelo Dbeaver por questão de hábito mesmo.)
![image](https://user-images.githubusercontent.com/63296032/210666015-1137f48a-8002-482e-94a2-f0dce82c60e6.png)
![image](https://user-images.githubusercontent.com/63296032/210667663-b09e2865-fcf6-48fb-9e57-2f49a121dfc4.png)

* Após a conexão do Dbeaver eu criei a base de dados de acordo com o snippet de código que foi disponibilizado.
![image](https://user-images.githubusercontent.com/63296032/210668123-a10915c6-4082-4194-8080-17bebb8639c5.png)
* Com a base criada e populada, fiz uma query inicial para analisar os dados: 
```sql
select
con.contrato_id,
con.ativo,
con.percentual,
con.cliente_id,
cli.nome,
tra.transacao_id,
tra.valor_total,
tra.percentual_desconto

from desafio_engenheiro.dbo.contrato con
left join desafio_engenheiro.dbo.transacao tra on tra.contrato_id = con.contrato_id
left join desafio_engenheiro.dbo.cliente cli on cli.cliente_id = con.cliente_id
```
![image](https://user-images.githubusercontent.com/63296032/210669136-561d13a9-8b55-4211-a56d-20b44454067d.png)
* Com esses dados, já consigo analisar e prototipar uma query que me traga as colunas que eu preciso 
```sql
select
con.contrato_id,
con.ativo,
con.percentual,
con.cliente_id,
cli.nome,
tra.transacao_id,
tra.valor_total,
tra.percentual_desconto,
(case when tra.percentual_desconto is null then 0 else tra.percentual_desconto end) as percentual_de_desconto, -- criei essa coluna para facilitar o cálculo e fazer o tratamento do valor nulo que havia nessa coluna.
tra.valor_total*((case when tra.percentual_desconto is null then 0 else tra.percentual_desconto end)/100) as valor_descontado, -- para descobrir o valor descontado precisei utilizar uma fórmula matemática (valor total * (percentual_de_desconto/100))
tra.valor_total-(tra.valor_total*((case when tra.percentual_desconto is null then 0 else tra.percentual_desconto end)/100)) as valor_final, -- com o resultado acima, consigo saber o valor final subtraindo o valor total do valor descontado
((tra.valor_total-(tra.valor_total*((case when tra.percentual_desconto is null then 0 else tra.percentual_desconto end)/100)))*con.percentual)/100 as ganho -- assim, consigo calcular o ganho por cliente com outra fórmula ((valor_final * percentual))/100

from desafio_engenheiro.dbo.contrato con
left join desafio_engenheiro.dbo.transacao tra on tra.contrato_id = con.contrato_id
left join desafio_engenheiro.dbo.cliente cli on cli.cliente_id = con.cliente_id
where con.ativo = '1' and (transacao_id IS NOT NULL) -- este where é responsável por me trazer somente os contratos que estão ativos e não são valores nulos
```
![image](https://user-images.githubusercontent.com/63296032/210670377-48185ec4-ae85-41f9-aef3-683f0ad6b417.png)
* Para deixar o código mais limpo e de acordo com o entregável eu utilizei uma CTE para agrupar a coluna de cliente e trazer exatamente o resultado que a questão pede:
```sql
with cte
as
(
select
cli.nome as cliente_nome,
((tra.valor_total-(tra.valor_total*((case when tra.percentual_desconto is null then 0 else tra.percentual_desconto end)/100)))*con.percentual)/100 as valor -- aqui todas as fórmulas do código anterior estão agrupadas em uma única linha

from desafio_engenheiro.dbo.contrato con
left join desafio_engenheiro.dbo.transacao tra on tra.contrato_id = con.contrato_id
left join desafio_engenheiro.dbo.cliente cli on cli.cliente_id = con.cliente_id
where con.ativo = '1' and transacao_id IS NOT NULL
)

SELECT
cte.cliente_nome,
sum (cte.valor) as valor
from cte
group by cte.cliente_nome

```
![image](https://user-images.githubusercontent.com/63296032/210670736-67f272b5-5a91-4943-a80d-d7266634445a.png)

* A diferença na última casa decimal é por conta de que o Dbeaver não arredonda valores

## Calcular o total líquido da empresa.
* Para Fazer isso eu criei um código no [Google Colab](https://colab.research.google.com/drive/1i2zi_jtIXPADtKcLxaJ7xQeLVmiSI6It#scrollTo=zQPfF_dLZDKN&uniqifier=1) onde fiz os comentários para resolução do problema.
```python
#instalando pyspark
!pip install pyspark

#Importando bibliotecas do spark
from pyspark import SparkContext
from pyspark import SparkConf

#Criando cluster
sc = SparkContext.getOrCreate()

#Criando RDD(Resilient Distributed Dataset): 
transacoes = [{'transacao_id':1, 'total_bruto':3000, 'desconto_percentual':6.99},
              {'transacao_id':2, 'total_bruto':57989, 'desconto_percentual':1.45},
              {'transacao_id':4, 'total_bruto':1, 'desconto_percentual':None},
              {'transacao_id':5, 'total_bruto':34, 'desconto_percentual':0.0}]

rdd = sc.parallelize(transacoes)

#Calculando o total líquido da transação:
def calcular_total_liquido(transacao):
  total_bruto = transacao['total_bruto']
  desconto_percentual = transacao['desconto_percentual']
  
  # Se o desconto_percentual for None, assume que é 0
  if desconto_percentual is None:
    desconto_percentual = 0
  
  #Utilizei a mesma fórmula matemática do código SQL
  total_liquido = (total_bruto - (total_bruto * (desconto_percentual/100)))*100 
  return total_liquido

rdd_liquido = rdd.map(calcular_total_liquido)

#Somando o total líquido:
total_liquido = rdd_liquido.sum()
print(total_liquido)
```
* A diferença na última casa decimal é pelo mesmo motivo do Dbeaver não arredondar as casas decimais
![image](https://user-images.githubusercontent.com/63296032/210816486-5abb8ea6-2e36-4d0a-9048-65200b0857f5.png)

## Transformação de dados disponíveis em arquivo Json
* Para essa etapa eu também vou utilizar o [Google Colab](https://colab.research.google.com/drive/125i94Mtu_iVxBbxfIcKcj50_kvMQYjd9) e de início vou fazer o upload do arquivo [Json](https://drive.google.com/file/d/1IDCjpDZh5St97jw4K_bAewJ8hf-rax9C/view?usp=sharing) para dentro da pasta de aquivo do colab:

![image](https://user-images.githubusercontent.com/63296032/210842949-2e9b9848-3aa6-416a-8556-8f03d394b7c6.png)
* para processar como dataframe, eu vou utilizar o [Pandas](https://pandas.pydata.org)
```python
# Instalando o Pandas e o Flatten(Flatten é uma operação que transforma uma matriz multidimensional em uma matriz unidimensional)
!pip install pandas
!pip install flatten_json

# Importando bibliotecas
import json
import pandas as pd
from pprint import pprint
from flatten_json import flatten

# Abrindo o arquivo 'data'
with open("data.json") as file:
  data = json.load(file)

# Exibe o arquivo para análise
pprint(data)

# Criando e exibindo o Data Frame para ver como o arquivo se comporta 
df = pd.read_json("data.json")

df.head()

# Podemos ver que a coluna ItemList é uma biblioteca com vários índices, e para expandir e normalizar eles no mesmo dataframe vou utilizar a biblioteca do Flatten
dict_flatten = (flatten(d) for d in data) #para cada elemento do json, vou chamar o flatten para gerar ele

# Chamando o generator para criar os elementos acima
dict_flatten

# Transformando o arquivo acima para Data Frame
df = pd.DataFrame(dict_flatten)

# Exibe o arquivo tratado
df.head()
```
![image](https://user-images.githubusercontent.com/63296032/210857858-45b12262-bba8-4380-ad46-507d8ed0320c.png)

 ## Arquitetura de ingestão dos dados de nota fiscal do entregável anterior
 Uma possível arquitetura de ingestão de dados de nota fiscal utilizando tecnologias da GCP poderia ser a seguinte:
 
 * Esquema de fluxo de dados:

A API de notas fiscais disponibiliza os dados em formato JSON através de uma URL.
Um aplicativo ou script em execução em uma máquina virtual ou em um contêiner em um cluster Kubernetes no GCP faz uma solicitação HTTP para a API e recebe os dados em formato JSON.
O aplicativo ou script transforma os dados em um formato compatível com o BigTable.
O aplicativo ou script carrega os dados transformados no BigTable através de uma conexão de API do Cloud BigTable.

* Funcionamento:

O aplicativo ou script é executado periodicamente, por exemplo, a cada hora, através de uma tarefa do Cloud Scheduler.
A tarefa do Cloud Scheduler envia um sinal para o aplicativo ou script para iniciar a ingestão de dados.
O aplicativo ou script faz uma solicitação HTTP para a API de notas fiscais para obter os dados em formato JSON.
O aplicativo ou script transforma os dados em um formato compatível com o BigTable, por exemplo, convertendo campos de data para o formato ISO 8601.
O aplicativo ou script se conecta ao Cloud BigTable através de uma conexão de API e carrega os dados transformados no BigTable.

* Tecnologias do ecossistema GCP:

*Cloud BigTable: Armazenamento de dados no formato de tabela.*

*Cloud Scheduler: Execução periódica de tarefas.*

*Máquina virtual ou cluster Kubernetes: Execução do aplicativo ou script que faz a solicitação HTTP para a API e carrega os dados no BigTable.*

![image](https://user-images.githubusercontent.com/63296032/210862154-a56ed492-bd46-4a19-baa8-0c9979f92e35.png)
