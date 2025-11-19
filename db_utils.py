from snowflake.snowpark import Session
from snowflake.snowpark.exceptions import SnowparkSQLException
import pandas as pd
import streamlit as st

# --- CONFIGURAÇÃO ---
CONNECTION_PARAMETERS = {
    "account": "IKCVERM-GEA41633",
    "user": "joaomata",
    "password": "Oscarwilde2018",
    "warehouse": "COMPUTE_WH",
    "database": "BD2",
    "schema": "PUBLIC"
}
# --------------------

def get_snowpark_session():
    if 'snowpark_session' not in st.session_state:
        try:
            session = Session.builder.configs(CONNECTION_PARAMETERS).create()
            session.sql("ALTER SESSION SET QUOTED_IDENTIFIERS_IGNORE_CASE = TRUE").collect()
            st.session_state.snowpark_session = session
            return session
        except Exception as e:
            st.error(f"Erro ao conectar com Snowpark. Verifique as credenciais em snowpark_utils.py: {e}")
            st.stop()
    return st.session_state.snowpark_session

def executar_snowpark_dml(session: Session, sql, params=None):
    try:
        if params:
            safe_params = tuple(f"'{p}'" if isinstance(p, str) else str(p) for p in params)
            sql_final = sql.replace('%s', '{}').format(*safe_params)
        else:
            sql_final = sql

        resultado = session.sql(sql_final).collect()
        return resultado[0][0] 
    except SnowparkSQLException as e:
        return str(e)
    except Exception as e:
        return str(e)

def executar_snowpark_select(session: Session, sql, params=None):
    try:
        if params:
            safe_params = tuple(f"'{p}'" if isinstance(p, str) else str(p) for p in params)
            sql_final = sql.replace('%s', '{}').format(*safe_params)
        else:
            sql_final = sql
            
        df_snowpark = session.sql(sql_final).to_pandas()
        return df_snowpark.columns.tolist(), df_snowpark.values.tolist()
    except SnowparkSQLException as e:
        st.error(f"Erro na consulta: {e}")
        return [], []
    except Exception as e:
        st.error(f"Erro geral: {e}")
        return [], []

def buscar_ids_nomes(session: Session, tabela, nome_coluna='NOME'):
    sql = f"SELECT ID, {nome_coluna} FROM {tabela}"
    return executar_snowpark_select(session, sql)

def buscar_registro_por_id(session: Session, tabela, id_registro):
    sql = f"SELECT * FROM {tabela} WHERE ID = %s"
    _, dados = executar_snowpark_select(session, sql, (id_registro,))
    return dados[0] if dados else None

def deletar_registro_por_id(session: Session, tabela, id_registro):
    sql = f"DELETE FROM {tabela} WHERE ID = %s"
    return executar_snowpark_dml(session, sql, (id_registro,))

def criar_pessoa(session: Session, nome, email, telefone, tipo_pessoa):
    sql = "INSERT INTO PESSOAS (nome, email, telefone, tipo_pessoa) VALUES (%s, %s, %s, %s)"
    return executar_snowpark_dml(session, sql, (nome, email, telefone, tipo_pessoa))

def ler_pessoas(session: Session):
    sql = "SELECT id, nome, email, telefone, tipo_pessoa FROM PESSOAS"
    return executar_snowpark_select(session, sql)

def atualizar_pessoa(session: Session, id_pessoa, nome, email, telefone, tipo_pessoa):
    sql = "UPDATE PESSOAS SET nome = %s, email = %s, telefone = %s, tipo_pessoa = %s WHERE id = %s"
    return executar_snowpark_dml(session, sql, (nome, email, telefone, tipo_pessoa, id_pessoa))

def criar_evento(session: Session, nome, data_inicio, data_fim, local, organizador_id):
    sql = "INSERT INTO EVENTOS (nome, data_inicio, data_fim, local, organizador_id) VALUES (%s, %s, %s, %s, %s)"
    return executar_snowpark_dml(session, sql, (nome, data_inicio, data_fim, local, organizador_id))

def ler_eventos(session: Session):
    sql = """
    SELECT
        E.id, E.nome AS nome_evento, E.data_inicio, E.data_fim, E.local, P.nome AS organizador
    FROM
        EVENTOS E
    JOIN
        PESSOAS P ON E.organizador_id = P.id
    """
    return executar_snowpark_select(session, sql)

def atualizar_evento(session: Session, id_evento, nome, data_inicio, data_fim, local, organizador_id):
    sql = "UPDATE EVENTOS SET nome = %s, data_inicio = %s, data_fim = %s, local = %s, organizador_id = %s WHERE id = %s"
    return executar_snowpark_dml(session, sql, (nome, data_inicio, data_fim, local, organizador_id, id_evento))

def criar_palestra(session: Session, titulo, descricao, data, hora, sala, evento_id, palestrante_id):
    sql = "INSERT INTO PALESTRAS (titulo, descricao, data, hora, sala, evento_id, palestrante_id) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    return executar_snowpark_dml(session, sql, (titulo, descricao, data, hora, sala, evento_id, palestrante_id))

def ler_palestras(session: Session):
    sql = """
    SELECT
        L.id, L.titulo, L.data, L.hora, L.sala, E.nome AS evento, P.nome AS palestrante
    FROM
        PALESTRAS L
    JOIN
        EVENTOS E ON L.evento_id = E.id
    JOIN
        PESSOAS P ON L.palestrante_id = P.id
    """
    return executar_snowpark_select(session, sql)

def atualizar_palestra(session: Session, id_palestra, titulo, descricao, data, hora, sala, evento_id, palestrante_id):
    sql = "UPDATE PALESTRAS SET titulo = %s, descricao = %s, data = %s, hora = %s, sala = %s, evento_id = %s, palestrante_id = %s WHERE id = %s"
    return executar_snowpark_dml(session, sql, (titulo, descricao, data, hora, sala, evento_id, palestrante_id, id_palestra))

def criar_inscricao(session: Session, participante_id, palestra_id, data_inscricao):
    sql = "INSERT INTO INSCRICOES (participante_id, palestra_id, data_inscricao) VALUES (%s, %s, %s)"
    return executar_snowpark_dml(session, sql, (participante_id, palestra_id, data_inscricao))

def ler_inscricoes(session: Session):
    sql = """
    SELECT
        I.participante_id, I.palestra_id, P.nome AS participante, L.titulo AS palestra, I.data_inscricao
    FROM
        INSCRICOES I
    JOIN
        PESSOAS P ON I.participante_id = P.id
    JOIN
        PALESTRAS L ON I.palestra_id = L.id
    """
    return executar_snowpark_select(session, sql)

def deletar_inscricao(session: Session, participante_id, palestra_id):
    sql = "DELETE FROM INSCRICOES WHERE participante_id = %s AND palestra_id = %s"
    return executar_snowpark_dml(session, sql, (participante_id, palestra_id))

def criar_pagamento(session: Session, participante_id, evento_id, valor, status, tipo_pagamento_id):
    sql = "INSERT INTO PAGAMENTOS (participante_id, evento_id, valor, status, tipo_pagamento_id) VALUES (%s, %s, %s, %s, %s)"
    return executar_snowpark_dml(session, sql, (participante_id, evento_id, valor, status, tipo_pagamento_id))

def ler_pagamentos(session: Session):
    sql = """
        SELECT
            PG.id, P.nome AS participante, E.nome AS evento, PG.valor, PG.status, T.nome AS tipo_pagamento, PG.TIPO_PAGAMENTO_ID
        FROM
            PAGAMENTOS PG
        JOIN
            PESSOAS P ON PG.participante_id = P.id
        JOIN
            EVENTOS E ON PG.evento_id = E.id
        LEFT JOIN
            TIPOS_PAGAMENTO T ON PG.TIPO_PAGAMENTO_ID = T.ID
    """
    return executar_snowpark_select(session, sql)

def atualizar_pagamento(session: Session, id_pagamento, valor, status, tipo_pagamento_id):
    sql = "UPDATE PAGAMENTOS SET valor = %s, status = %s, tipo_pagamento_id = %s WHERE id = %s"
    return executar_snowpark_dml(session, sql, (valor, status, tipo_pagamento_id, id_pagamento))

def criar_tipo_pagamento(session: Session, nome):
    sql = "INSERT INTO TIPOS_PAGAMENTO (nome) VALUES (%s)"
    return executar_snowpark_dml(session, sql, (nome,))

def ler_tipos_pagamento(session: Session):
    sql = "SELECT id, nome FROM TIPOS_PAGAMENTO"
    return executar_snowpark_select(session, sql)

def atualizar_tipo_pagamento(session: Session, id_tipo, nome):
    sql = "UPDATE TIPOS_PAGAMENTO SET nome = %s WHERE id = %s"
    return executar_snowpark_dml(session, sql, (nome, id_tipo))

def upsert_feedback(session: Session, participante_id, palestra_id, nota, comentario):
    sql = f"""
    MERGE INTO FEEDBACK_PALESTRAS AS target
    USING (
        SELECT {participante_id} AS participante_id, {palestra_id} AS palestra_id, {nota} AS nota, '{comentario}' AS comentario
    ) AS source
    ON target.participante_id = source.participante_id AND target.palestra_id = source.palestra_id
    WHEN MATCHED THEN
        UPDATE SET target.nota = source.nota, target.comentario = source.comentario
    WHEN NOT MATCHED THEN
        INSERT (participante_id, palestra_id, nota, comentario)
        VALUES (source.participante_id, source.palestra_id, source.nota, source.comentario)
    """
    return executar_snowpark_dml(session, sql)

def ler_feedback(session: Session):
    sql = """
    SELECT
        F.id, P.nome AS participante, L.titulo AS palestra, F.nota, F.comentario
    FROM
        FEEDBACK_PALESTRAS F
    JOIN
        PESSOAS P ON F.participante_id = P.id
    JOIN
        PALESTRAS L ON F.palestra_id = L.id
    """
    return executar_snowpark_select(session, sql)

def atualizar_feedback(session: Session, id_feedback, nota, comentario):
    sql = "UPDATE FEEDBACK_PALESTRAS SET nota = %s, comentario = %s WHERE id = %s"
    return executar_snowpark_dml(session, sql, (nota, comentario, id_feedback))

def consulta_participantes_palestra(session: Session):
    sql = """
    SELECT
        P.nome AS participante,
        L.titulo AS palestra,
        E.nome AS evento,
        I.data_inscricao
    FROM
        INSCRICOES I
    JOIN PESSOAS P ON I.participante_id = P.id
    JOIN PALESTRAS L ON I.palestra_id = L.id
    JOIN EVENTOS E ON L.evento_id = E.id
    ORDER BY E.nome, L.titulo
    """
    return executar_snowpark_select(session, sql)

def consulta_aninhada_1_nao_inscritos_em_evento_x(session: Session, evento_id_ref):
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
    return executar_snowpark_select(session, sql, (evento_id_ref,))

def consulta_aninhada_2_palestras_acima_media(session: Session):
    sql = """
    SELECT
        titulo,
        (SELECT AVG(nota) FROM FEEDBACK_PALESTRAS F2 WHERE F2.palestra_id = P.id) AS media_palestra
    FROM
        PALESTRAS P
    WHERE
        (SELECT AVG(nota) FROM FEEDBACK_PALESTRAS F WHERE F.palestra_id = P.id) > (
            SELECT AVG(nota) FROM FEEDBACK_PALESTRAS
        )
    ORDER BY media_palestra DESC;
    """
    return executar_snowpark_select(session, sql)

def consulta_grupo_1_total_eventos_por_organizador(session: Session):
    sql = """
    SELECT
        O.nome AS organizador,
        COUNT(DISTINCT E.id) AS total_eventos_organizados,
        COALESCE(SUM(P.valor), 0.00) AS valor_total_arrecadado
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
    return executar_snowpark_select(session, sql)

def consulta_grupo_2_estatisticas_por_status_pagamento(session: Session):
    sql = """
    SELECT
        status,
        COUNT(*) AS total_pagamentos,
        AVG(valor) AS valor_medio,
        MAX(valor) AS maior_valor,
        MIN(valor) AS menor_valor
    FROM
        PAGAMENTOS
    GROUP BY
        status;
    """
    return executar_snowpark_select(session, sql)

def consulta_conjunto_1_atores_financeiros(session: Session):
    sql = """
    SELECT nome, email, 'ORGANIZADOR' AS tipo_financeiro
    FROM PESSOAS WHERE tipo_pessoa = 'Organizador'
    
    UNION
    
    SELECT P.nome, P.email, 'PARTICIPANTE PAGANTE' AS tipo_financeiro
    FROM PESSOAS P
    JOIN PAGAMENTOS PG ON P.id = PG.participante_id
    WHERE P.tipo_pessoa = 'Participante';
    """
    return executar_snowpark_select(session, sql)

def consulta_conjunto_2_palestras_sem_feedback(session: Session):
    sql = """
    SELECT DISTINCT L.id, L.titulo
    FROM PALESTRAS L
    JOIN INSCRICOES I ON L.id = I.palestra_id
    
    EXCEPT
    
    SELECT DISTINCT L.id, L.titulo
    FROM PALESTRAS L
    JOIN FEEDBACK_PALESTRAS F ON L.id = F.palestra_id;
    """
    return executar_snowpark_select(session, sql)