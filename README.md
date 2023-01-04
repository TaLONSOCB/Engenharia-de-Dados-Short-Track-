# [Engenharia-de-Dados-Short-Track](https://github.com/AhirtonLopes/teste_eng_dados#teste---engenharia-de-dados-short-track)
Essa é minha tentativa de solução para esse teste técnico

### Programas utilizados:
* [Google Cloud Sql](https://cloud.google.com/sql?hl=pt-br)
* [SqlServer](https://www.microsoft.com/pt-br/sql-server)
* [Dbeaver](https://dbeaver.io/download/)
* [Diagrams](https://www.diagrams.net)

## Escrever uma aplicação para calcular o ganho total da empresa.
* Para Fazer isso eu criei um banco SQL Server no Cloud SQL e configurei uma conexão para ele autorizar o ip público da minha máquina, para eu conseguir acessar o banco utilizando o [Dbeaver](https://dbeaver.io/download/)
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

A diferença na última casa decimal é por conta de que o Dbeaver não arredonda valores

