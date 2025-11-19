# db_utils.py (Versão Completa e Atualizada com 7 CRUDs)

import snowflake.connector
import datetime

# --- CONFIGURAÇÃO ---
# Substitua com suas credenciais REAIS do Snowflake
SNOWFLAKE_CONFIG = {
    'user': 'joaomata',
    'password': 'Oscarwilde2018',
    'account': 'IKCVERM-GEA41633',
    'warehouse': 'COMPUTE_WH',
    'database': 'BD2',
    'schema': 'PUBLIC'
}
# --------------------

def conectar_bd():
    """Tenta estabelecer a conexão com o Snowflake."""
    try:
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        return conn
    except snowflake.connector.errors.ProgrammingError as e:
        print(f"Erro ao conectar ao Snowflake: {e}")
        print("Verifique se as credenciais e a rede estão corretas.")
        return None
    except Exception as e:
        print(f"Ocorreu um erro inesperado: {e}")
        return None

def executar_sql(sql_query, params=None, fetch=False):
    conn = conectar_bd()
    if conn is None:
        # Se a conexão falhar, retorna uma tupla vazia para evitar o erro de desempacotamento
        return (None, None) if fetch else "Erro de Conexão com Snowflake."

    try:
        cursor = conn.cursor()
        cursor.execute(sql_query, params)

        if fetch:
            columns = [col[0] for col in cursor.description]
            resultados = cursor.fetchall()
            return columns, resultados
        else:
            conn.commit()
            return cursor.rowcount

    except snowflake.connector.errors.ProgrammingError as e:
        print(f"Erro SQL: {e}")
        if fetch:
             return ([], [])
        else:
             return str(e)

    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if conn:
            conn.close()

# --- FUNÇÕES READ (Auxiliares para FKs) ---

def buscar_organizadores():
    """Busca ID e Nome dos Organizadores (e Palestrantes) para FKs."""
    sql = "SELECT id, nome FROM PESSOAS WHERE tipo_pessoa = 'Organizador';"
    return executar_sql(sql, fetch=True)

def buscar_participantes():
    """Busca ID e Nome das Pessoas que são Participantes para FKs."""
    sql = "SELECT id, nome FROM PESSOAS WHERE tipo_pessoa = 'Participante';"
    return executar_sql(sql, fetch=True)

def buscar_eventos():
    """Busca ID e Nome dos Eventos para FKs."""
    sql = "SELECT id, nome FROM EVENTOS ORDER BY data_inicio DESC;"
    return executar_sql(sql, fetch=True)

def buscar_todas_palestras():
    """Busca ID e Título de todas as Palestras para FKs."""
    sql = "SELECT id, titulo FROM PALESTRAS ORDER BY data DESC, hora ASC;"
    return executar_sql(sql, fetch=True)

def buscar_tipos_pagamento():
    """Busca ID e Nome dos Tipos de Pagamento para FKs."""
    sql = "SELECT id, nome FROM TIPOS_PAGAMENTO ORDER BY nome ASC;"
    return executar_sql(sql, fetch=True)


# --- CRUD PARA PESSOAS ---

def criar_pessoa(nome, email, telefone, tipo_pessoa):
    sql = """
    INSERT INTO PESSOAS (nome, email, telefone, tipo_pessoa)
    VALUES (%s, %s, %s, %s)
    """
    return executar_sql(sql, (nome, email, telefone, tipo_pessoa))


# --- CRUD PARA EVENTOS ---

def criar_evento(nome, data_inicio, data_fim, local, organizador_id):
    sql = """
    INSERT INTO EVENTOS (nome, data_inicio, data_fim, local, organizador_id)
    VALUES (%s, %s, %s, %s, %s)
    """
    return executar_sql(sql, (nome, data_inicio, data_fim, local, organizador_id))

def ler_eventos():
    sql = """
    SELECT E.id, E.nome AS Evento, E.data_inicio, E.data_fim, E.local, P.nome AS Organizador
    FROM EVENTOS E JOIN PESSOAS P ON E.organizador_id = P.id
    ORDER BY E.data_inicio DESC;
    """
    return executar_sql(sql, fetch=True)

def atualizar_evento(event_id, nome, data_inicio, data_fim, local, organizador_id):
    sql = """
    UPDATE EVENTOS
    SET nome = %s, data_inicio = %s, data_fim = %s, local = %s, organizador_id = %s
    WHERE id = %s
    """
    return executar_sql(sql, (nome, data_inicio, data_fim, local, organizador_id, event_id))

def deletar_evento(event_id):
    sql = "DELETE FROM EVENTOS WHERE id = %s"
    return executar_sql(sql, (event_id,))


# --- CRUD PARA PALESTRAS ---

def criar_palestra(titulo, descricao, data, hora, sala, evento_id, palestrante_id):
    sql = """
    INSERT INTO PALESTRAS (titulo, descricao, data, hora, sala, evento_id, palestrante_id)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    return executar_sql(sql, (titulo, descricao, data, hora, sala, evento_id, palestrante_id))

def ler_palestras():
    sql = """
    SELECT L.id, L.titulo, E.nome AS Evento, P.nome AS Palestrante, L.data, L.hora, L.sala, L.evento_id, L.palestrante_id
    FROM PALESTRAS L
    JOIN EVENTOS E ON L.evento_id = E.id
    JOIN PESSOAS P ON L.palestrante_id = P.id
    ORDER BY L.data DESC, L.hora ASC;
    """
    return executar_sql(sql, fetch=True)

def buscar_palestrantes():
    """Busca ID e Nome das Pessoas que podem Palestrar (tipo Organizador)."""
    sql = "SELECT id, nome FROM PESSOAS WHERE tipo_pessoa = 'Organizador';"
    return executar_sql(sql, fetch=True)

def buscar_palestra_por_id(palestra_id):
    sql = "SELECT titulo, descricao, data, hora, sala, evento_id, palestrante_id FROM PALESTRAS WHERE id = %s"
    return executar_sql(sql, (palestra_id,), fetch=True)

def atualizar_palestra(palestra_id, titulo, descricao, data, hora, sala, evento_id, palestrante_id):
    sql = """
    UPDATE PALESTRAS
    SET titulo = %s, descricao = %s, data = %s, hora = %s, sala = %s, evento_id = %s, palestrante_id = %s
    WHERE id = %s
    """
    return executar_sql(sql, (titulo, descricao, data, hora, sala, evento_id, palestrante_id, palestra_id))

def deletar_palestra(palestra_id):
    sql = "DELETE FROM PALESTRAS WHERE id = %s"
    return executar_sql(sql, (palestra_id,))


# --- CRUD PARA INSCRICOES ---

def criar_inscricao(participante_id, palestra_id, data_inscricao):
    sql = """
    INSERT INTO INSCRICOES (participante_id, palestra_id, data_inscricao)
    VALUES (%s, %s, %s)
    """
    return executar_sql(sql, (participante_id, palestra_id, data_inscricao))

def ler_inscricoes():
    sql = """
    SELECT I.participante_id, I.palestra_id, P.nome AS Participante, L.titulo AS Palestra, I.data_inscricao
    FROM INSCRICOES I
    JOIN PESSOAS P ON I.participante_id = P.id
    JOIN PALESTRAS L ON I.palestra_id = L.id
    ORDER BY I.data_inscricao DESC;
    """
    return executar_sql(sql, fetch=True)

def deletar_inscricao(participante_id, palestra_id):
    sql = "DELETE FROM INSCRICOES WHERE participante_id = %s AND palestra_id = %s"
    return executar_sql(sql, (participante_id, palestra_id))


# --- CRUD PARA PAGAMENTOS ---

def criar_pagamento(participante_id, evento_id, valor, status):
    sql = """
    INSERT INTO PAGAMENTOS (participante_id, evento_id, valor, status)
    VALUES (%s, %s, %s, %s)
    """
    return executar_sql(sql, (participante_id, evento_id, valor, status))

def ler_pagamentos():
    sql = """
    SELECT PG.id, P.nome AS Participante, E.nome AS Evento, PG.valor, PG.status
    FROM PAGAMENTOS PG
    JOIN PESSOAS P ON PG.participante_id = P.id
    JOIN EVENTOS E ON PG.evento_id = E.id
    ORDER BY PG.id DESC;
    """
    return executar_sql(sql, fetch=True)
    
def buscar_pagamento_por_id(pagamento_id):
    sql = "SELECT participante_id, evento_id, valor, status FROM PAGAMENTOS WHERE id = %s"
    return executar_sql(sql, (pagamento_id,), fetch=True)

def atualizar_pagamento(pagamento_id, participante_id, evento_id, valor, status):
    sql = "UPDATE PAGAMENTOS SET participante_id = %s, evento_id = %s, valor = %s, status = %s WHERE id = %s"
    return executar_sql(sql, (participante_id, evento_id, valor, status, pagamento_id))

def deletar_pagamento(pagamento_id):
    sql = "DELETE FROM PAGAMENTOS WHERE id = %s"
    return executar_sql(sql, (pagamento_id,))


# --- CRUD PARA TIPOS_PAGAMENTO ---

def criar_tipo_pagamento(nome):
    sql = "INSERT INTO TIPOS_PAGAMENTO (nome) VALUES (%s)"
    return executar_sql(sql, (nome,))

def ler_tipos_pagamento():
    sql = "SELECT id, nome FROM TIPOS_PAGAMENTO ORDER BY nome ASC;"
    return executar_sql(sql, fetch=True)

def atualizar_tipo_pagamento(tipo_id, nome):
    sql = "UPDATE TIPOS_PAGAMENTO SET nome = %s WHERE id = %s"
    return executar_sql(sql, (nome, tipo_id))

def deletar_tipo_pagamento(tipo_id):
    sql = "DELETE FROM TIPOS_PAGAMENTO WHERE id = %s"
    return executar_sql(sql, (tipo_id,))


# --- CRUD PARA FEEDBACK_PALESTRAS ---

def upsert_feedback(participante_id, palestra_id, nota, comentario):
    """
    UPSERT: Usa MERGE para atualizar o feedback se ele já existir,
    ou insere um novo se for o primeiro.
    """
    sql = """
    MERGE INTO FEEDBACK_PALESTRAS AS target
    USING (
        SELECT %s AS participante_id, %s AS palestra_id, %s AS nota, %s AS comentario
    ) AS source
    ON target.participante_id = source.participante_id AND target.palestra_id = source.palestra_id
    WHEN MATCHED THEN
        UPDATE SET target.nota = source.nota, target.comentario = source.comentario
    WHEN NOT MATCHED THEN
        INSERT (participante_id, palestra_id, nota, comentario)
        VALUES (source.participante_id, source.palestra_id, source.nota, source.comentario)
    """
    params = (participante_id, palestra_id, nota, comentario)
    return executar_sql(sql, params)

def ler_feedback():
    sql = """
    SELECT F.id, P.nome AS Participante, L.titulo AS Palestra, F.nota, F.comentario
    FROM FEEDBACK_PALESTRAS F
    JOIN PESSOAS P ON F.participante_id = P.id
    JOIN PALESTRAS L ON F.palestra_id = L.id
    ORDER BY F.id DESC;
    """
    return executar_sql(sql, fetch=True)

def buscar_feedback_por_id(feedback_id):
    sql = "SELECT participante_id, palestra_id, nota, comentario FROM FEEDBACK_PALESTRAS WHERE id = %s"
    return executar_sql(sql, (feedback_id,), fetch=True)

def atualizar_feedback(feedback_id, participante_id, palestra_id, nota, comentario):
    sql = """
    UPDATE FEEDBACK_PALESTRAS
    SET participante_id = %s, palestra_id = %s, nota = %s, comentario = %s
    WHERE id = %s
    """
    return executar_sql(sql, (participante_id, palestra_id, nota, comentario, feedback_id))

def deletar_feedback(feedback_id):
    sql = "DELETE FROM FEEDBACK_PALESTRAS WHERE id = %s"
    return executar_sql(sql, (feedback_id,))


# --- CONSULTAS FASE 3/4 ---
def consulta_participantes_palestra():
    """Consulta de 3+ tabelas (Fase 3)."""
    sql = """
    SELECT
        P.nome AS Participante,
        L.titulo AS Palestra,
        E.nome AS Evento,
        I.data_inscricao
    FROM
        INSCRICOES I
    JOIN PESSOAS P ON I.participante_id = P.id
    JOIN PALESTRAS L ON I.palestra_id = L.id
    JOIN EVENTOS E ON L.evento_id = E.id
    WHERE
        P.tipo_pessoa = 'Participante'
    ORDER BY
        E.nome, L.titulo, P.nome;
    """
    return executar_sql(sql, fetch=True)

def consulta_aninhada_1_nao_inscritos_em_evento_x(evento_id_ref):
    """Fase 4 - Consulta Aninhada 1: Participantes que NÃO se inscreveram em NENHUMA palestra do Evento X."""
    sql = """
    SELECT
        nome, email
    FROM
        PESSOAS
    WHERE
        tipo_pessoa = 'Participante'
        AND id NOT IN (
            SELECT DISTINCT
                participante_id
            FROM
                INSCRICOES I
            JOIN PALESTRAS L ON I.palestra_id = L.id
            WHERE
                L.evento_id = %s
        );
    """
    return executar_sql(sql, (evento_id_ref,), fetch=True)

def consulta_aninhada_2_palestras_acima_media():
    """Fase 4 - Consulta Aninhada 2: Palestras com nota média de feedback superior à média geral."""
    sql = """
    SELECT
        titulo,
        (SELECT AVG(nota) FROM FEEDBACK_PALESTRAS F2 WHERE F2.palestra_id = P.id) AS media_palestra
    FROM
        PALESTRAS P
    WHERE
        (SELECT AVG(nota) FROM FEEDBACK_PALESTRAS F WHERE F.palestra_id = P.id) > (
            -- Subconsulta 1: Média geral de todas as notas
            SELECT AVG(nota) FROM FEEDBACK_PALESTRAS
        )
    ORDER BY media_palestra DESC;
    """
    return executar_sql(sql, fetch=True)


def consulta_grupo_1_total_eventos_por_organizador():
    """Fase 4 - Consulta de Grupo 1: Contagem de eventos e valor total de pagamentos por Organizador."""
    sql = """
    SELECT
        O.nome AS Organizador,
        COUNT(DISTINCT E.id) AS Total_Eventos_Organizados,
        COALESCE(SUM(P.valor), 0.00) AS Valor_Total_Arrecadado
    FROM
        PESSOAS O
    LEFT JOIN EVENTOS E ON O.id = E.organizador_id
    LEFT JOIN PAGAMENTOS P ON E.id = P.evento_id
    WHERE
        O.tipo_pessoa = 'Organizador'
    GROUP BY
        O.nome
    HAVING
        COUNT(DISTINCT E.id) > 0;
    """
    return executar_sql(sql, fetch=True)

def consulta_grupo_2_estatisticas_por_status_pagamento():
    """Fase 4 - Consulta de Grupo 2: Média, Máximo e Mínimo dos valores de pagamento por status."""
    sql = """
    SELECT
        status,
        COUNT(*) AS Total_Pagamentos,
        AVG(valor) AS Valor_Medio,
        MAX(valor) AS Maior_Valor,
        MIN(valor) AS Menor_Valor
    FROM
        PAGAMENTOS
    GROUP BY
        status;
    """
    return executar_sql(sql, fetch=True)


def consulta_conjunto_1_atores_financeiros():
    """Fase 4 - Consulta de Conjunto 1 (UNION): Organizadores OU Participantes que pagaram."""
    sql = """
    -- Parte 1: Todos os Organizadores
    SELECT nome, email, 'ORGANIZADOR' AS Tipo_Financeiro
    FROM PESSOAS WHERE tipo_pessoa = 'Organizador'
    
    UNION
    
    -- Parte 2: Participantes que fizeram algum pagamento
    SELECT P.nome, P.email, 'PARTICIPANTE PAGANTE' AS Tipo_Financeiro
    FROM PESSOAS P
    JOIN PAGAMENTOS PG ON P.id = PG.participante_id
    WHERE P.tipo_pessoa = 'Participante';
    """
    return executar_sql(sql, fetch=True)

def consulta_conjunto_2_palestras_sem_feedback():
    """Fase 4 - Consulta de Conjunto 2 (EXCEPT): Palestras que tiveram inscrições, mas NÃO receberam feedback."""
    sql = """
    -- Parte 1: Palestras que possuem inscrições
    SELECT DISTINCT L.id, L.titulo
    FROM PALESTRAS L
    JOIN INSCRICOES I ON L.id = I.palestra_id
    
    EXCEPT
    
    -- Parte 2: Palestras que possuem feedback
    SELECT DISTINCT L.id, L.titulo
    FROM PALESTRAS L
    JOIN FEEDBACK_PALESTRAS F ON L.id = F.palestra_id;
    """
    return executar_sql(sql, fetch=True)