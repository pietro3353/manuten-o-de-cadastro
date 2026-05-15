# 📘 Relatório Técnico: Hardening Pipeline de Automação B3 (Versão 6)

## 1. Resumo Executivo das Implementações

Esta versão transformou o pipeline B3 de um script de força bruta em um sistema robusto, auditável e seguro. O objetivo principal foi implementar salvaguardas (*guardrails*) para garantir a integridade do banco de dados Oracle da B3, reduzir execuções desnecessárias e adicionar observabilidade completa.

As melhorias foram divididas em 5 frentes principais:

1. **SQL Transacional e Resiliente**
   - O SQL gerado deixou de ser uma sequência linear e passou a ser estruturado em blocos `PL/SQL`.
   - Adição de `SAVEPOINT` por CNPJ: se um fundo falhar, apenas as operações dele sofrem rollback, permitindo que os demais continuem.
   - Adição de `COMMIT` condicional: o commit total só ocorre se a taxa de erros for inferior a 5% (configurável). Caso contrário, o banco sofre `ROLLBACK` completo.
2. **Motor de Delta (Diff Processing)**
   - O pipeline agora compara os dados atuais da CVM com a execução anterior.
   - Gera o arquivo `update_fundos_DELTA.sql` contendo **apenas** os fundos que são novos ou sofreram alterações, reduzindo drasticamente a carga no banco de dados nas execuções diárias (de 1.5M de linhas para apenas o que mudou).
   - Mantém um arquivo histórico automático na pasta `archive/`.
3. **Validação Rigorosa de Dados (Data Quality)**
   - Validação de CNPJs usando o algoritmo de módulo 11. Se o CNPJ do fundo for inválido, ele é sumariamente ignorado da geração SQL.
   - Detecção e sanitização de caracteres de controle (que quebram SQL) e padrões básicos de SQL Injection.
   - Geração de um **Data Quality Report** detalhado e embutido no JSON de saída, listando exatamente quais fundos foram rejeitados e o motivo.
4. **Métricas e Observabilidade**
   - Coleta de tempo de execução (*timing*) milissegundo a milissegundo para cada etapa do fluxo (download, consolidação, validação, etc.).
   - Criação de logs históricos em `logs/executions.jsonl`.
   - Criação de endpoints HTTP (`/metrics`, `/diff`, `/health`) para monitoramento externo.
5. **Automação no n8n**
   - Atualização do workflow n8n (V6) para execução 100% automatizada (Cron Trigger às 6h da manhã).
   - Inclusão de nó de tratamento de erros global que envia alertas por e-mail em caso de falha.

---

## 2. Arquitetura de Arquivos

O código que antes era monolítico foi modularizado para facilitar manutenção.

### `main.py`
**O que faz:** É o maestro (orquestrador) do pipeline e servidor HTTP.
**Como funciona:** 
- Configura o servidor HTTP que aguarda os comandos do n8n (`/start`, `/status`, `/download-sql`, etc.).
- Na execução, coordena a chamada para download dos arquivos da CVM e ANBIMA.
- Chama as funções de consolidação em memória.
- Delega as tarefas complexas (validação, diff, geração SQL) para os módulos auxiliares.
**Outputs principais:** Orquestra a geração de `resultado_cvm.json`, `update_fundos.sql` e a resposta JSON para o n8n.

### `pipeline_helpers.py`
**O que faz:** Concentra a lógica de geração de SQL transacional e gestão das métricas.
**Como funciona:**
- Recebe a lista de fundos validados e constrói o cabeçalho PL/SQL, os blocos de `SAVEPOINT` e as condições de `COMMIT`/`ROLLBACK`.
- Possui a função `generate_delta_sql` que filtra os dados com base no diff.
- Consolida os dados de tempo e qualidade em um JSON padronizado de métricas.

### `data_validator.py`
**O que faz:** Motor de qualidade de dados.
**Como funciona:**
- Ao passar um fundo pela função `validate_fund`, verifica formato de datas, valida CNPJs com matemática de módulo 11, checa limite de caracteres da razão social e purifica os campos de texto contra injeção de código.
- Retorna um diagnóstico para cada fundo, indicando se ele está apto para ir ao banco de dados ou se deve ser barrado.

### `diff_engine.py`
**O que faz:** O "cérebro" das atualizações incrementais.
**Como funciona:**
- Antes do sistema sobrescrever o JSON da CVM do dia anterior, este módulo faz backup do arquivo antigo para a pasta `archive/`.
- Compara campo a campo cada um dos ~87 mil fundos antigos com os novos.
- Categoriza fundos como *novos*, *alterados* ou *inalterados*, listando exatamente o que mudou.
**Outputs:** Gera `diff_report.json` e o resumo legível `diff_summary.txt`.

### `Bootstrap_Cadastro_Fundos_-_n8n_V3_(integração_completa_com_Python_via_polling).n8n`
**O que faz:** O workflow orquestrador do n8n.
**Como funciona:** É a cola entre o ambiente agendado e o script Python. Diariamente às 6h (Cron), ou via gatilho manual, ele detecta se o servidor Python está acessível (local ou Docker), envia o comando `/start`, fica perguntando se acabou (`/status`) e, ao final, faz o download do SQL pronto (`/download-sql`) para aplicar no banco. Se algo falhar, envia e-mail.

---

## 3. Ambiente Docker para Homologação

O Docker permite encapsular o `main.py` e todos os seus módulos e dependências (como a versão exata do Python e pacotes como `httpx`) em uma "bolha" padronizada. 

### Por que usar o Docker?
A ideia do Docker aqui é funcionar como um **ambiente de homologação idêntico à produção**. Antes de subir qualquer alteração no código Python para o servidor Windows nativo da B3, você testa no Docker. Se funcionar na bolha do Docker, a garantia de que funcionará no servidor B3 é quase 100%, pois isola problemas do seu computador (como bibliotecas instaladas globalmente, versões de sistema operacional, etc.).

### O que tem na pasta `docker_env/`?
- **`Dockerfile`**: É a receita de bolo. Diz: "Baixe o Linux com Python 3.12, instale o `requirements.txt`, copie os arquivos `.py` e crie as pastas `data/`, `logs/` e `archive/`".
- **`docker-compose.yml`**: É o maestro do Docker. Ele define os limites de segurança para a homologação (ex: no máximo 2GB de RAM), configura a porta de acesso (`8000`) e liga a pasta física `data/` do seu computador com a pasta de dados dentro do container, garantindo que os relatórios e SQLs apareçam no seu Windows.

### Como utilizar o Docker para testes

1. **Abra o terminal** e navegue até a pasta do docker:
   ```bash
   cd "C:\Users\pietr\Documents\Automaçao b3\docker_env"
   ```

2. **Crie/Atualize o container** (Sempre que você alterar um arquivo `.py` na raiz e copiar para o `docker_env`, precisa rodar esse comando para o Docker "aprender" o código novo):
   ```bash
   docker-compose build --no-cache
   ```

3. **Suba o servidor em background**:
   ```bash
   docker-compose up -d
   ```

4. **Inicie o workflow no n8n**:
   Com o container rodando, você pode ir no n8n e clicar em "Execute Workflow". O n8n vai encontrar o Python rodando na porta 8000 do Docker e executar o processo completo.

5. **Verifique os outputs**:
   Os arquivos `resultado_cvm.json`, `update_fundos_DELTA.sql`, etc., aparecerão "magicamente" na pasta `docker_env/data/` do seu Windows, graças ao mapeamento de volumes do Docker.

6. **Para parar o container e limpar**:
   ```bash
   docker-compose down
   ```

### 🔁 Fluxo de Vida das Alterações
1. Edite o código e teste rapidamente no Windows (`python main.py --limit 5`).
2. Ficou bom? Copie os `.py` alterados para a pasta `docker_env/`.
3. Rode o `docker-compose build` e depois `up -d`.
4. Dispare pelo n8n para simular a produção real.
5. Tudo perfeito? O código está validado para subir para o servidor da B3.
