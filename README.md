# Weather Traffic

# Documentação do Projeto
Criei uma documentação detalhada que explica todas as decisões que tomei ao longo deste projeto.

Para conferir todos os detalhes e entender o motivo por trás de cada decisão que tomei, você pode acessar o seguinte link para o Notion:

[Documentação do Projeto no Notion](https://east-canopy-5bc.notion.site/Projeto-Weather-Traffic-87b817a2047b48cabd280e9a522c3fc8)

Além disso, para sua conveniência, também disponibilizei um arquivo chamado documentation.odt no repositório do projeto, que contém o mesmo conteúdo. Você pode acessá-lo caso o site do Notion esteja temporariamente indisponível.

# Setup
Este projeto utiliza o Apache Airflow para orquestrar as tarefas ETL. Para configurar o ambiente do Airflow e executar este projeto, siga as instruções na [documentação oficial do Apache Airflow](https://airflow.apache.org/docs/apache-airflow/stable/installation/index.html).

Certifique-se de instalar todas as dependências necessárias e configurar as variáveis de ambiente conforme descrito na documentação.

O projeto foi desenvolvido e testado na versão 2.6.2 do Apache Airflow. Recomendo o uso desta versão para evitar possíveis conflitos.

## Adicionar as DAGs ao Apache Airflow

Para executar as DAGs deste projeto no seu ambiente Apache Airflow, siga estas etapas:

1. Certifique-se de que você configurou o ambiente do Apache Airflow conforme as instruções da documentação oficial.

2. Installe todas as bibliotecas que estão no airflow_requirements no seu Airflow.

3. A pasta `dags` deste repositório contém as DAGs que definem as tarefas e fluxos de trabalho do Airflow para o projeto, coloque esses arquivos dentro da pasta dag do seu setup do Airflow

4. Serão necessárias duas variáveis de ambiente para que as ETLs funcionem corretamente. Não as incluí no código por motivos de segurança e para evitar que outras pessoas tenham acesso às minhas chaves pessoais. As variáveis são as seguintes:

    - `google_directions_api_key`: Você pode obter esta chave seguindo os passos descritos na [Documentação da API Directions do Google](https://developers.google.com/maps/documentation/directions/overview).
    
    - `openweathermap_api_key`: Você pode obter esta chave seguindo os passos descritos na [Documentação da API OpenWeatherMap](https://openweathermap.org/api).

Certifique-se de configurar essas variáveis de ambiente no seu ambiente Airflow para que as DAGs funcionem corretamente.

5. Antes de rodar qualquer outra DAG, é importante executar a DAG `database.init_config`. Esta DAG é responsável por criar o banco de dados e a modelagem inicial das tabelas necessárias para o projeto.

6. Feito isso, você pode rodar qualquer uma das ETLs disponíveis. Elas funcionarão e salvarão os dados no banco de dados que foi criado na Etapa 5.

## Como visualizar os graficos

Para rodar o `analise.ipynb` siga as seguintes etapas:

1. **Organizando a Estrutura de Diretórios:**

   Antes de prosseguir, é recomendável que você copie a pasta `graficos` deste repositório e a cole no mesmo diretório onde você possui o seu projeto Apache Airflow, **ao lado**, e não dentro da pasta de configuração do Airflow. Isso facilitará o acesso ao banco de dados e evitará a necessidade de alterar o comando `create_engine` no código para especificar um caminho diferente para o banco de dados. Certifique-se de que a estrutura de diretórios esteja organizada dessa maneira antes de continuar.

2. **Criação do Ambiente Virtual (Opcional, mas Recomendado):**

   Para isolar as dependências do projeto, é recomendável criar um ambiente virtual. Use o comando `venv` para criar um novo ambiente virtual. Substitua `myenv` pelo nome que desejar para o ambiente. Execute o seguinte comando no seu terminal:

   ```bash
   python -m venv myenv

3. **Ativação do Ambiente Virtual:**

   Ative o ambiente virtual de acordo com o seu sistema operacional:

   - **Unix/Linux**:

     ```bash
     source myenv/bin/activate
     ```

   - **Windows (PowerShell)**:

     ```powershell
     .\myenv\Scripts\Activate
     ```

   Certifique-se de executar o comando apropriado para o sistema operacional que você está usando. Isso ativará o ambiente virtual.

4. **Navegação até a Pasta `graficos`:**

   Abra um terminal e navegue até a pasta onde o arquivo `analise.ipynb` está localizado. Você pode usar o comando `cd` para isso.

5. **Instalação de Dependências:**

   Dentro da pasta `graficos`, existe um arquivo `requirements.txt` que lista as dependências necessárias para executar o `analise.ipynb`. Use o `pip` para instalar essas dependências no ambiente virtual. Execute o seguinte comando:

   ```bash
   pip install -r requirements.txt

6. **Executando o notebook:**
   
    Após a instalação das dependências e a configuração do ambiente conforme as etapas anteriores, você está pronto para executar o `analise.ipynb`. Este notebook contém código para ler o banco de dados, fazer consultas nas tabelas gerar     gráficos com os dados.

