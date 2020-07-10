# Boas vindas ao repositório do projeto One For All!

Você já usa o GitHub diariamente para desenvolver os exercícios, certo? Agora, para desenvolver os projetos, você deverá seguir as instruções a seguir. Fique atento a cada passo e, se tiver qualquer dúvida, nos envie por Slack! #vqv 🚀

Aqui você vai encontrar os detalhes de como estruturar o desenvolvimento do seu projeto a partir desse repositório, utilizando uma branch específica e um Pull Request para colocar seus códigos.

## O que deverá ser desenvolvido

Hoje, no projeto ***One For All***, você usará **uma** tabela para revisar e consolidar **todos** os principais conceitos vistos até o momento.

Você receberá uma tabela não normalizada, a qual deverá ser normalizada e populada, para que você possa executar queries sobre ela com o intuito de encontrar as informações solicitadas.

---

## Como desenvolver e entregar este projeto

A seguir, tem-se a apresentação de um problema que deve ser resolvido através da normalização e criação de queries. Cada passo solicita um tipo de resposta diferente. Para cada desafio, crie um arquivo SQL e inclua sua solução nele.

1. Leia a pergunta e crie um arquivo chamado `desafioN.sql`, em que N é o número do desafio (exemplo: desafio3.sql).

2. O arquivo deve conter apenas o código SQL do desafio resolvido. **Não se esqueça de incluir o ponto e vírgula (";")** no final de suas queries e também de **colocar o nome do banco_de_dados.tabela por completo**, como no exemplo a seguir.

```sql
SELECT * FROM spotifyclone.artista;
```

3. Atente-se a detalhes de digitação em seu código. Qualquer diferença em nomes, apelidos, ou CAIXA ALTA e caixa baixa podem invalidar suas respostas.

4. Faça isso até finalizar todos os desafios e depois siga as instruções de como entregar o projeto em [**Instruções para entregar seu projeto**](#instruções-para-entregar-seu-projeto)

---

## Problema a ser resolvido

Você receberá uma tabela não normalizada semelhante ao que poderia ser uma versão simplificada do banco de dados do Spotify. O trabalho consistirá de duas partes:

1. Normalizar essa tabela, criar o schema no seu banco de dados local e populá-lo;

2. Realizar os desafios no seu banco normalizado e populado.

**Aviso:** Todos os dados da tabela abaixo são fictícios e foram gerados aleatoriamente através de ferramentas, com o intuito deixar o projeto mais real para vocês 😉. Quaisquer similaridades com informações reais são meras coincidências.

**Observação:** O banco de dados real do Spotify é muito mais completo que a tabela abaixo. No entanto, use APENAS as informações passadas e solicitadas aqui. Não crie dados que não foram solicitados.

#### Normalize as tabelas para a 3ª Forma Normal

Abaixo você pode visualizar e baixar uma planilha com as tabelas que deverão ser normalizadas:

![Tabela não normalizada "Spotify Clone"](./images/non-normalized-table.png)
[Faça o download dela aqui](./SpotifyClone-Non-NormalizedTable.xlsx)

Antes de tudo, você deverá normalizar essas tabelas para a 3° Forma Normal. Monte quantas planilhas e tabelas forem necessárias para criar essa versão normalizada. Não é necessário criar o código SQL neste momento.

Embora não seja necessário, é recomendado que você crie suas planilhas na sequência 1ª -> 2ª -> 3ª Forma Normal.

Caso haja dúvidas, consulte o material já passado até hoje para te auxiliar.

Deve ser possível encontrar as informações abaixo em suas tabelas:

* Informações sobre quais planos estão disponíveis e seus detalhes;

* Informações sobre todos os artistas e seus detalhes (use informação abaixo);

* Informações sobre todos os álbuns de cada artista (use informação abaixo);

* Informações sobre todas as canções de cada álbum (use informação abaixo);

* Informações sobre planos que um usuário ou uma usuária possui;

* A relação que cada usuário ou usuária possui com um plano;

* A relação que cada artista possui com um álbum;

* A relação que cada canção possui com um álbum;

* A relação de quais artistas um usuário ou uma usuária está seguindo;

* A relação de todas as canções reproduzidas por um usuário ou uma usuária.

Após ter deixado as tabelas na 3ª Forma Normal, faça quaisquer modificações que forem necessárias em suas tabelas para que elas possam ser capazes de armazenar as informações descritas abaixo:

* Artista "***Walter Phoenix***" criou o álbum "***Envious***" com as canções *"Soul For Us"*,*"Reflections Of Magic"* e *"Dance With Her Own"*;

* Artista "***Walter Phoenix***" criou o álbum "***Exuberant***" com as canções *"Troubles Of My Inner Fire"* e *"Time Fireworks"*;

* Artista "***Peter Strong***" criou o álbum "***Hallowed Steam***" com as canções *"Magic Circus"*, *"Honey, So Do I"*,*"Sweetie, Let's Go Wild"* e *"She Knows"*;

* Artista "***Lance Day***" crio o álbum "***Incandescent***" com as canções *"Fantasy For Me"*, *"Celebration Of More"*, *"Rock His Everything"*, *"Home Forever"*, *"Diamond Power"* e *"Honey,Let's Be Silly"*;

* Artista "***Freedie Shannon***" criou o álbum "***Temporary Culture***" com as canções *"Thang Of Thunder"*, *"Words Of Her Life"* e *"Without My Streets"*.

Caso sua estrutura ainda não atenda a esses requisitos, volte à estrutura que foi criada no passo anterior e faça as alterações necessárias para que suas tabelas possuam essas informações.

---

## Desafios SQL

##### Desafio 1 - Transforme Suas planilhas em Código SQL

Seu desafio agora é pegar toda a estrutura que você criou na seção anterior e transformá-la em código SQL. Os detalhes estão a seguir:

* Crie um banco com o nome de **`SpotifyClone`**;

* Providencie as queries necessárias para criar tabelas normalizadas que atendam aos requisitos descritos na seção anterior;

* Providencie as queries necessárias para popular as tabelas de acordo com os dados listados na seção anterior.

Crie somente um arquivo `SQL`. Seu código deverá ser similar ao seguinte:

```sql
CREATE DATABASE IF NOT EXISTS SpotifyClone;

USE SpotifyClone;

CREATE TABLE tabela1(
    coluna1 tipo restricoes,
    coluna2 tipo restricoes,
    colunaN tipo restricoes,
) engine = InnoDB;

CREATE TABLE tabela2(
    coluna1 tipo restricoes,
    coluna2 tipo restricoes,
    colunaN tipo restricoes,
) engine = InnoDB;

INSERT INTO tabela1 (coluna1, coluna2)
       VALUES ('exemplo de dados 1', 'exemplo de dados A'),
              ('exemplo de dados 2', 'exemplo de dados B'),
              ('exemplo de dados 3', 'exemplo de dados C');

INSERT INTO tabela2 (coluna1, coluna2)
       VALUES ('exemplo de dados 1', 'exemplo de dados X'),
              ('exemplo de dados 2', 'exemplo de dados Y');
```

---

##### Desafio 2

Crie uma `VIEW` chamada `estatisticas_musicais` que exiba três colunas. Cada coluna da `VIEW` deve exibir quantas vezes algo está cadastrado no banco de dados. Exiba a quantidade de canções na primeira coluna e dê a essa coluna o apelido "**cancoes**". A segunda coluna deve exibir a quantidade de artistas, e deverá ter o apelido "**artistas**". A terceira coluna deve exibir a quantidade de álbuns, e deverá ter o apelido "**albuns**".

Sua `VIEW` deve retornar a seguinte informação ao ser consultada:

![Estatísticas musicais](./images/estatisticas_musicais.png)

---

##### Desafio 3

Crie uma `VIEW` chamada `historico_reproducao_usuarios`. Essa `VIEW` deverá ter apenas duas colunas. A primeira coluna deve ser apelidada de "**usuario**" e deve exibir o nome do usuário ou da usuária. A segunda coluna deve ser apelidada de "**nome**" e exibir o nome da canção ouvida por aquele usuário ou usuária com base no seu seu histórico de reprodução. Os resultados devem estar ordenados por nome de usuário ou usuária em ordem crescente.

Sua `VIEW` deve retornar a seguinte informação, ao ser consultada:

![Histórico de reprodução dos usuários e usuárias](./images/HistoricoReproducaoUsuarios.png)

---

##### Desafio 4

Crie uma `VIEW` com o nome `top_3_artistas` que exiba o código identificador, o nome e a quantidade de pessoas que segue este artista. Sua `VIEW` deve mostrar somente os três artistas mais populares no banco `SpotifyClone`.

A coluna que representa o código identificador do artista deve ser apelidada de "**artista_id**". A coluna que contém o nome do artista deve ser apelidada de "**artista**". A coluna com a quantidade de pessoas que estão seguindo aquele artista deve receber o apelido "**seguidores**".

Seu resultado deve estar ordenado em ordem decrescente, baseando-se na quantidade de seguidores.

Sua `VIEW` deve retornar a seguinte informação, ao ser consultada:

![Top 3 artistas](./images/top_3_artistas.png)

---

##### Desafio 5

Estamos fazendo um estudo das músicas mais tocadas e precisamos saber quais são as duas músicas mais tocadas no momento. Crie uma `VIEW` chamada `top_2_hits_do_momento` que deve exibir as informações sobre o nome da canção e quantas vezes ela foi tocada recentemente (considere todas canções registradas no banco como "recentemente"). Dê à coluna que possui o nome da canção o apelido "**cancao**" e a que possui a quantidade de vezes que foi tocada o apelido "**reproducoes**".

Seu resultado deve estar ordenado em ordem decrescente, baseando-se no número de reproduções. Queremos apenas o top 2 de músicas mais tocadas.

Sua `VIEW` deve retornar a seguinte informação, ao ser consultada:

![Top 2 hits do momento](./images/top_2_hits_do_momento.png)

---

##### Desafio 6

Queremos algumas informações sobre o faturamento da empresa. Tenha como base o valor dos planos e o plano que cada usuário ou usuária cadastrado possui no banco. Crie uma `VIEW` chamada `faturamento_atual` que deve exibir quatro dados. A primeira coluna deve exibir o mínimo faturado com planos da Spotify, com o apelido de "**faturamento_minimo**". A segunda coluna deve exibir o valor máximo faturado, com o apelido de "**faturamento_maximo**". A terceira coluna deve exibir o valor médio faturado até o momento, com o apelido de "**faturamento_medio**". Por fim, a quarta coluna deve exibir o faturamento total, com o apelido de "**faturamento_total**".

Sua `VIEW` deve retornar a seguinte informação, ao ser consultada:

![Faturamento atual](./images/faturamento_atual.png)

---

##### Desafio 7

Mostre uma relação de todos os álbuns produzidos por cada artista, juntamente com a quantidade de seguidores que aquele artista possui, de acordo com os detalhes a seguir. Crie uma `VIEW` chamada `perfil_artistas` que, na primeira coluna, deve exibir a valor identificador do artista, com o apelido "**artista_id**". A segunda coluna deve exibir o nome do artista, com o apelido "**artista**". A terceira coluna deve exibir o valor identificador do álbum, com o apelido "**album_id**". A quarta coluna deve exibir o nome do álbum, com o apelido "**album**". A quinta coluna deve exibir a quantidade de seguidores que aquele artista possui e deve ser apelidada de "**seguidores**". 

Seus resultados devem estar ordenados em ordem decrescente, baseando-se no número de seguidores.

Sua `VIEW` deve retornar a seguinte informação, ao ser consultada:

![PerfilArtistas](./images/perfil_artistas.png)

---

##### Desafio 8

Crie uma trigger chamada `trigger_usuario_delete` que deve ser disparada sempre que um usuário ou uma usuária for excluído(a) do banco de dados. Sua trigger deve executar a seguintes ações abaixo, em ordem:

1. Remover informações sobre este usuário ou esta usuária da tabela que armazena quem ele ou ela estava seguindo;

2. Remover informações sobre este usuário ou esta usuária do histórico de reprodução;

3. Remover informações sobre este usuário ou esta usuária da tabela que mostra a relação do usuário com seu plano.

Teste a funcionalidade correta de sua trigger, fazendo a exclusão da usuária 15.

Sua trigger deve acarretar a exclusão do usuário ou da usuária que for passado(a) em todas as tabelas mencionadas acima. Em consequência disso, todas referências devem ser excluídas, como na imagem abaixo:

![trigger_usuario_delete](./images/trigger_usuario_delete.png)

---

##### Desafio 9

Crie uma procedure chamada `albuns_do_artista` que recebe como parâmetro o valor da coluna identificadora de um artista e em retorno deve exibir as seguintes quatro colunas: o código identificador do artista, com o apelido "**artista_id**", o nome do artista, com o apelido "**artista**", o valor identificador do álbum produzido por aquele artista, com o apelido "**album_id**" e o nome do álbum, com o apelido "**album**".

Confirme a execução correta da procedure, chamando-a e passando o valor "1" como parâmetro. Sua procedure deve retornar o mesmo que o resultado abaixo:

`CALL albuns_do_artista(1)`

![Álbuns do artista](./images/AlbunsDoArtista.png)

---

##### Desafio 10

Crie uma function chamada de `quantidade_musicas_no_historico` que exibe a quantidade de músicas que estão presente atualmente no histórico de reprodução de um usuário ou uma usuária. Ao receber o código identificador daquele usuário ou daquela usuária, exiba a quantidade de canções em seu histórico de reprodução.

Sua procedure deve retornar o mesmo resultado da imagem ao receber o valor "17" como parâmetro:

![Quantidade de músicas no histórico](./images/quantidade_musicas_no_historico.png)

---

##### Desafio 11

Crie uma `VIEW` chamada "*CancoesPremium*" que exiba o nome e a quantidade de vezes que cada canção foi tocada por usuários e usuárias do plano familiar ou universitário, de acordo com os detalhes a seguir.

A primeira coluna deve exibir o nome da canção, com o apelido "**nome**". A segunda coluna deve exibir a quantidade de reproduções que aquela canção recebeu, com o apelido "**reproducoes**".

Seus resultados devem agrupar as canções através de seu nome e devem estar ordenados por nome da canção em ordem crescente.

Sua `VIEW` deve retornar a seguinte informação, ao ser consultada:

![cancoes_premium](./images/cancoes_premium.png)

---

## Instruções para entregar seu projeto:

### ANTES DE COMEÇAR A DESENVOLVER:

1. Clone o repositório
  * `git clone https://github.com/tryber/sd-0x-mysql-one-for-all.git`.
  * Entre na pasta do repositório que você acabou de clonar:
    * `cd sd-0x-mysql-one-for-all`

2. Crie uma branch a partir da branch `master`
  * Verifique que você está na branch `master`
    * Exemplo: `git branch`
  * Se não estiver, mude para a branch `master`
    * Exemplo: `git checkout master`
  * Agora, crie uma branch onde você vai guardar os `commits` do seu projeto
    * Você deve criar uma branch no seguinte formato: `nome-de-usuario-nome-do-projeto`
    * Exemplo: `git checkout -b jhonatan-de-souza-one-for-all`

3. Para cada exercício, você deve criar um novo arquivo SQL seguindo a seguinte estrutura:
  * desafio1.sql, desafio2.sql, desafio3.sql, desafio4.sql, ..., desafioN.sql

4. Adicione as mudanças ao _stage_ do Git e faça um `commit`
  * Verifique que as mudanças ainda não estão no _stage_
    * Exemplo: `git status` (deve aparecer o arquivo que você alterou como desafio1.sql)
  * Adicione o novo arquivo ao _stage_ do Git
      * Exemplo:
        * `git add .` (adicionando solução para desafio 1)
        * `git status` (deve aparecer listado o arquivo _desafio1.sql_ em verde)
  * Faça o `commit` inicial
      * Exemplo:
        * `git commit -m 'Iniciando o projeto One for All'` (fazendo o primeiro commit)
        * `git status` (deve aparecer uma mensagem tipo _nothing to commit_ )

6. Adicione a sua branch com o novo `commit` ao repositório remoto
  * Usando o exemplo anterior: `git push -u origin seunome-one-for-all`

7. Crie um novo `Pull Request` _(PR)_
  * Vá até a página de _Pull Requests_ do [repositório no GitHub](https://github.com/tryber/sd-0x-mysql-one-for-all/pulls)
  * Clique no botão verde _"New pull request"_
  * Clique na caixa de seleção _"Compare"_ e escolha a sua branch **com atenção**
  * Clique no botão verde _"Create pull request"_
  * Adicione uma descrição para o _Pull Request_ e clique no botão verde _"Create pull request"_
  * **Não se preocupe em preencher mais nada por enquanto!**
  * Volte até a [página de _Pull Requests_ do repositório](https://github.com/tryber/sd-0x-mysql-one-for-all/pulls) e confira que o seu _Pull Request_ está criado

---

### DURANTE O DESENVOLVIMENTO

* ⚠ **LEMBRE-SE DE CRIAR TODOS OS ARQUIVOS DENTRO DA PASTA COM O SEU NOME** ⚠

* Faça `commits` das alterações que você fizer no código regularmente

* Lembre-se de sempre após um (ou alguns) `commits` atualizar o repositório remoto

* Os comandos que você utilizará com mais frequência são:
    1. `git status` _(para verificar o que está em vermelho - fora do stage - e o que está em verde - no stage)_
    2. `git add` _(para adicionar arquivos ao stage do Git)_
    3. `git commit` _(para criar um commit com os arquivos que estão no stage do Git)_
    5. `git push -u nome-da-branch` _(para enviar o commit para o repositório remoto na primeira vez que fizer o `push` de uma nova branch)_
    4. `git push` _(para enviar o commit para o repositório remoto após o passo anterior)_

### Entregáveis

Para entregar o seu projeto você deverá criar um Pull Request neste repositório. Este Pull Request deverá conter os arquivos SQL dos desafios 1 ao 11.

Qualquer dúvida, procure a monitoria. Lembre-se que você pode consultar nosso conteúdo sobre Git & GitHub sempre que quiser!

---

### DEPOIS DE TERMINAR O DESENVOLVIMENTO

Para submeter o _Pull Request_ do seu projeto para avaliação, siga os passos a seguir:

* Vá até a página **DO SEU** _Pull Request_, adicione a label de _"code-review"_ e marque seus colegas
  * No menu à direita, clique no _link_ **"Labels"** e escolha a _label_ **code-review**
  * No menu à direita, clique no _link_ **"Assignees"** e escolha **o seu usuário**
  * No menu à direita, clique no _link_ **"Reviewers"** e digite `students`, selecione o time `tryber/students-sd-0x`

Se ainda houver alguma dúvida sobre como entregar seu projeto, [aqui tem um video explicativo](https://vimeo.com/362189205).

---

### REVISANDO UM PULL REQUEST

⚠⚠⚠

À medida que você e os outros estudantes forem entregando os projetos, vocês serão alertados **via Slack** para também fazer a revisão dos _Pull Requests_ dos seus colegas. Fiquem atentos às mensagens do _"Pull Reminders"_ no _Slack_!

Use o material que você já viu sobre [Code Review](https://course.betrybe.com/real-life-engineer/code-review/) para te ajudar a revisar os projetos que chegaram para você.
