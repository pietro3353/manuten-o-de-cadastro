# Usa uma imagem oficial e enxuta do Python
FROM python:3.11-slim

# Diretório base da aplicação
WORKDIR /app

# Definindo Timezone para evitar problemas de timestamp nas rotinas CVM/B3
ENV TZ=America/Sao_Paulo
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Copiar dependências da pasta docker_env (contexto será o diretório pai)
COPY docker_env/requirements.txt .

# Instalar dependências sem cache para economizar espaço
RUN pip install --no-cache-dir -r requirements.txt

# Copiar o script principal da raiz do projeto
COPY main.py .

# Criar o diretório de dados que será linkado com o volume do host
RUN mkdir -p /app/data

# Expor a porta 8000
EXPOSE 8000

# TRUQUE DE ENGENHARIA: Mudamos o WORKDIR para /app/data antes de executar o script.
# Como o main.py salva "resultado_cvm.json" e "update_fundos.sql" no diretório atual de execução,
# rodando a partir daqui os arquivos cairão automaticamente na pasta /app/data (que estará mapeada).
WORKDIR /app/data

# Comando de inicialização do servidor HTTP nativo
CMD ["python", "/app/main.py", "--server"]
