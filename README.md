# Demo Devin

- Author: Prof. Barbosa  
- Contact: infobarbosa@gmail.com  
- Github: [infobarbosa](https://github.com/infobarbosa)

## Objetivo
O objetivo desse laboratório é construir um pipeline de dados em PySpark simples utilizando o [Devin](https://devin.ai/).

## `AGENTS.md`
É fornecido uma arquivo `AGENTS.md` com as instruções que o Devin precisa seguir para o desenvolvimento.

## Arquivos de Exemplo
Você deve utilizar dois arquivos de exemplo: `clientes` e `pedidos`.

### `clientes.csv`
```sh
git clone https://github.com/infobarbosa/dataset-json-clientes ./data/input/dataset-json-clientes

```
O conteúdo desse arquivo está estruturado da seguinte forma:
```
./data/input/dataset-json-clientes/data/clientes.json
```

Sample desse arquivo:
```
{"id": 1, "nome": "Isabel Abreu", "data_nasc": "1982-10-26", "cpf": "512.084.739-05", "email": "isabel.abreusigycp@outlook.com", "interesses": ["Filmes"], "carteira_investimentos": {"FIIs": 11533.69, "CDB": 26677.01}}
{"id": 2, "nome": "Natália Ramos", "data_nasc": "1971-04-26", "cpf": "780.369.125-03", "email": "natalia.ramosrzmyqb@hotmail.com", "interesses": ["Viagens"], "carteira_investimentos": {}}
{"id": 3, "nome": "Larissa Garcia", "data_nasc": "2006-12-03", "cpf": "608.275.134-53", "email": "larissa.garciaviennn@outlook.com", "interesses": ["Livros"], "carteira_investimentos": {}}
{"id": 4, "nome": "Milena Freitas", "data_nasc": "2007-09-07", "cpf": "674.158.392-00", "email": "milena.freitasrgsswy@gmail.com", "interesses": ["Astronomia", "Lazer", "Religião"], "carteira_investimentos": {}}
{"id": 5, "nome": "Caleb Gonçalves", "data_nasc": "1989-06-05", "cpf": "703.465.219-80", "email": "caleb.goncalveslkcgfn@gmail.com", "interesses": ["Astronomia", "Música"], "carteira_investimentos": {"CDB": 13423.81, "Criptomoedas": 45986.93}}
```

### `pedidos.csv`
```sh
git clone https://github.com/infobarbosa/datasets-csv-pedidos ./data/input/datasets-csv-pedidos

```

O conteúdo dos arquivos está estruturado da seguinte forma:
```
./data/input/datasets-csv-pedidos/data/pedidos/pedidos-2026-01.csv
```

Sample desse arquivo:
```
ID_PEDIDO;PRODUTO;VALOR_UNITARIO;QUANTIDADE;DATA_CRIACAO;UF;ID_CLIENTE
f198e8f7-033d-414d-b032-20975e84edde;LIQUIDIFICADOR;300.0;1;2026-01-05T18:36:28;MG;8409
97969db5-9304-4b80-b19e-3a9d60ce6520;CELULAR;1000.0;3;2026-01-01T11:58:48;DF;934
f1db6c7e-0701-42fd-90b2-638b57cefe38;NOTEBOOK;1500.0;2;2026-01-17T15:28:57;MG;5872
3994d9fa-6609-4818-8efa-c3a570a6116a;GELADEIRA;2000.0;1;2026-01-27T13:37:31;MA;174
```
