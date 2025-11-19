# app.py (Versão Completa e Corrigida com 7 CRUDs)

import datetime
import streamlit as st
import pandas as pd

from db_utils import (
    conectar_bd, executar_sql,
    
    # CRUD PESSOAS
    criar_pessoa, 

    # CRUD EVENTOS
    ler_eventos, criar_evento, atualizar_evento, deletar_evento, buscar_organizadores,

    # CRUD PALESTRAS
    buscar_eventos, buscar_palestrantes, criar_palestra, ler_palestras, deletar_palestra,
    buscar_palestra_por_id, atualizar_palestra,

    # CRUD INSCRICOES
    buscar_participantes, buscar_todas_palestras, criar_inscricao, ler_inscricoes, deletar_inscricao,

    # CRUD PAGAMENTOS
    criar_pagamento, ler_pagamentos, atualizar_pagamento, deletar_pagamento, buscar_pagamento_por_id,

    # CRUD TIPOS_PAGAMENTO
    criar_tipo_pagamento, ler_tipos_pagamento, deletar_tipo_pagamento, atualizar_tipo_pagamento,

    # CRUD FEEDBACK_PALESTRAS
    upsert_feedback, ler_feedback, atualizar_feedback, deletar_feedback, buscar_feedback_por_id,

    # CONSULTAS
    consulta_participantes_palestra,
    consulta_aninhada_1_nao_inscritos_em_evento_x,
    consulta_aninhada_2_palestras_acima_media,
    consulta_grupo_1_total_eventos_por_organizador,
    consulta_grupo_2_estatisticas_por_status_pagamento,
    consulta_conjunto_1_atores_financeiros,
    consulta_conjunto_2_palestras_sem_feedback
)

st.set_page_config(layout="wide")
# --- Funções de Interface CRUD ---

def mostrar_crud_pessoas():
    st.header("1. 👥 Cadastro e Manutenção de Usuários")
    with st.form("form_pessoa_create"):
        st.subheader("Incluir Nova Pessoa")
        c1, c2, c3 = st.columns(3)
        nome = c1.text_input("Nome", key="c_nome")
        email = c2.text_input("Email", key="c_email")
        telefone = c3.text_input("Telefone", key="c_telefone")
        tipo = st.selectbox("Tipo", ['Participante', 'Organizador'], key="c_tipo")
        submit_button = st.form_submit_button("Cadastrar Pessoa")

        if submit_button:
            if nome and email and tipo:
                resultado = criar_pessoa(nome, email, telefone, tipo)
                if isinstance(resultado, int) and resultado > 0:
                    st.success(f"Pessoa '{nome}' cadastrada com sucesso!")
                    st.experimental_rerun()
                elif isinstance(resultado, str):
                    st.error(f"Erro ao cadastrar: {resultado}")
                else:
                    st.warning("Erro desconhecido ao cadastrar.")
            else:
                st.error("Nome, Email e Tipo são obrigatórios.")

    st.subheader("Consulta e Manutenção (READ, UPDATE, DELETE)")
    
    colunas, dados = executar_sql("SELECT id, nome, email, telefone, tipo_pessoa FROM PESSOAS;", fetch=True)
    
    if dados and colunas:
        df = pd.DataFrame(dados, columns=colunas)
        st.dataframe(df, use_container_width=True)
        
        st.caption("Para as demais operações (Alterar/Excluir), use os IDs acima:")
        col_upd, col_del = st.columns(2)
        
        with col_upd.form("form_pessoa_update"):
            st.markdown("✏️ **Alterar Pessoa (Nome)**")
            upd_id = st.number_input("ID da Pessoa a Alterar", min_value=1, step=1)
            upd_nome = st.text_input("Novo Nome")
            upd_button = st.form_submit_button("Alterar Dados")
            
            if upd_button:
                sql_update = f"UPDATE PESSOAS SET nome = '{upd_nome}' WHERE id = {upd_id}"
                resultado = executar_sql(sql_update)
                if isinstance(resultado, int) and resultado > 0:
                    st.success(f"Pessoa ID {upd_id} atualizada.")
                    st.experimental_rerun()
                elif isinstance(resultado, str):
                    st.error(f"Erro ao atualizar: {resultado}")
                else:
                    st.warning("Pessoa não encontrada ou dados não alterados.")


        with col_del.form("form_pessoa_delete"):
            st.markdown("❌ **Excluir Pessoa**")
            del_id = st.number_input("ID da Pessoa a Excluir", min_value=1, step=1, key="d_id")
            del_button = st.form_submit_button("Excluir Pessoa")

            if del_button:
                sql_delete = f"DELETE FROM PESSOAS WHERE id = {del_id}"
                resultado = executar_sql(sql_delete)
                if isinstance(resultado, int) and resultado > 0:
                    st.success(f"Pessoa ID {del_id} excluída com sucesso.")
                    st.experimental_rerun()
                elif isinstance(resultado, str):
                    st.error(f"Erro ao excluir: {resultado}")
                else:
                    st.warning("Pessoa não encontrada ou erro desconhecido.")
    else:
        st.info("Nenhuma pessoa cadastrada ou erro ao conectar ao BD.")


def mostrar_crud_eventos():
    st.header("2. 📋 Manutenção de Eventos")
    # ... (código de CRUD Eventos - mantido igual ao último envio) ...
    colunas, dados = ler_eventos()
    
    if dados and len(dados) > 0:
        df = pd.DataFrame(dados, columns=colunas)
        st.dataframe(df, use_container_width=True)
    else:
        st.info("Nenhum evento cadastrado ou erro ao buscar dados.")
        df = pd.DataFrame() 

    col_org, dados_org = buscar_organizadores()
    if not dados_org:
        st.warning("É necessário cadastrar um Organizador na seção 'CRUD Pessoas' para criar um evento.")
        organizadores_map = {}
        organizadores_list = []
    else:
        organizadores_map = {nome: id for id, nome in dados_org}
        organizadores_list = list(organizadores_map.keys())

    st.divider()

    with st.expander("➕ Incluir Novo Evento", expanded=False):
        with st.form("form_evento_create"):
            st.subheader("Dados do Evento")
            col1, col2 = st.columns(2)
            nome = col1.text_input("Nome do Evento")
            local = col2.text_input("Local do Evento")

            col3, col4 = st.columns(2)
            data_inicio = col3.date_input("Data de Início")
            data_fim = col4.date_input("Data de Fim (Opcional)", value=None)

            if organizadores_list:
                nome_org_selecionado = st.selectbox("Organizador Responsável", organizadores_list)
                organizador_id = organizadores_map[nome_org_selecionado]
            else:
                organizador_id = None
            
            submit_button = st.form_submit_button("Cadastrar Evento")

            if submit_button:
                if nome and data_inicio and organizador_id:
                    data_fim_str = str(data_fim) if data_fim else None
                    resultado = criar_evento(nome, str(data_inicio), data_fim_str, local, organizador_id)
                    
                    if isinstance(resultado, int) and resultado > 0:
                        st.success(f"Evento '{nome}' cadastrado com sucesso!")
                        st.experimental_rerun()
                    elif isinstance(resultado, str):
                        st.error(f"Erro ao cadastrar: {resultado}")
                    else:
                        st.warning("Erro desconhecido.")
                else:
                    st.error("Nome, Data de Início e Organizador são obrigatórios.")

    if not df.empty:
        st.divider()
        col_upd, col_del = st.columns(2)

        with col_upd.form("form_evento_update"):
            st.markdown("✏️ **Alterar Evento Existente**")
            update_id = st.selectbox("ID do Evento a Alterar", df['ID'].unique(), key="upd_event_id")
            upd_nome = st.text_input("Novo Nome")
            upd_data_inicio = st.date_input("Nova Data de Início")
            
            if organizadores_list:
                upd_nome_org = st.selectbox("Novo Organizador", organizadores_list, key="upd_org")
                upd_organizador_id = organizadores_map[upd_nome_org]
            else:
                upd_organizador_id = None

            upd_button = st.form_submit_button("Alterar Dados")

            if upd_button and upd_organizador_id:
                resultado = atualizar_evento(update_id, upd_nome, str(upd_data_inicio), None, None, upd_organizador_id)
                
                if isinstance(resultado, int) and resultado > 0:
                    st.success(f"Evento ID {update_id} atualizado.")
                    st.experimental_rerun()
                elif isinstance(resultado, str):
                    st.error(f"Erro ao atualizar: {resultado}")
                else:
                    st.warning("Nenhuma alteração realizada.")

        with col_del.form("form_evento_delete"):
            st.markdown("❌ **Excluir Evento**")
            delete_id = st.selectbox("ID do Evento a Excluir", df['ID'].unique(), key="del_id")
            del_button = st.form_submit_button("Excluir Evento")

            if del_button:
                resultado = deletar_evento(delete_id)
                if isinstance(resultado, int) and resultado > 0:
                    st.success(f"Evento ID {delete_id} excluído com sucesso.")
                    st.experimental_rerun()
                elif isinstance(resultado, str):
                    st.error(f"Erro ao excluir. Verifique se há Palestras ou Pagamentos ligados a este Evento.")
                else:
                    st.warning("Evento não encontrado.")


def mostrar_crud_palestras():
    st.header("3. 🎤 Cadastro de Palestras")
    # ... (código de CRUD Palestras - mantido igual ao último envio) ...
    _, dados_eventos = buscar_eventos()
    _, dados_palestrantes = buscar_organizadores() # Organizadores como palestrantes
    
    eventos_map = {nome: id for id, nome in dados_eventos} if dados_eventos else {}
    palestrantes_map = {nome: id for id, nome in dados_palestrantes} if dados_palestrantes else {}
    eventos_list = list(eventos_map.keys())
    palestrantes_list = list(palestrantes_map.keys())

    colunas, dados = ler_palestras()
    
    if dados and len(dados) > 0:
        df = pd.DataFrame(dados, columns=colunas)
        st.dataframe(df.drop(columns=['EVENTO_ID', 'PALESTRANTE_ID']), use_container_width=True) 
    else:
        st.info("Nenhuma palestra cadastrada ou erro ao buscar dados.")
        df = pd.DataFrame()

    st.divider()

    if not eventos_list or not palestrantes_list:
        st.error("🚨 É necessário cadastrar pelo menos um Evento e um Organizador/Palestrante para continuar.")
        return

    tab1, tab2, tab3 = st.tabs(["➕ Cadastrar", "✏️ Alterar", "❌ Excluir"])

    # --- CREATE ---
    with tab1:
        with st.form("form_palestra_create"):
            st.subheader("Dados da Nova Palestra")
            
            c1, c2 = st.columns(2)
            titulo = c1.text_input("Título da Palestra")
            sala = c2.text_input("Sala/Local")

            descricao = st.text_area("Descrição")
            
            c3, c4 = st.columns(2)
            data = c3.date_input("Data")
            hora = c4.time_input("Hora (HH:MM)")
            
            c5, c6 = st.columns(2)
            nome_evento = c5.selectbox("Evento", eventos_list)
            nome_palestrante = c6.selectbox("Palestrante", palestrantes_list)

            submit_button = st.form_submit_button("Cadastrar Palestra")

            if submit_button:
                evento_id = eventos_map[nome_evento]
                palestrante_id = palestrantes_map[nome_palestrante]
                hora_str = hora.strftime('%H:%M:%S')                
                resultado = criar_palestra(titulo, descricao, str(data), hora_str, sala, evento_id, palestrante_id)
                
                if isinstance(resultado, int) and resultado > 0:
                    st.success(f"Palestra '{titulo}' cadastrada com sucesso!")
                    st.experimental_rerun()
                elif isinstance(resultado, str):
                    st.error(f"Erro ao cadastrar: {resultado}")
                else:
                    st.warning("Erro desconhecido.")


    # --- UPDATE ---
    with tab2:
        with st.form("form_palestra_update"):
            st.subheader("Alterar Palestra Existente")
            
            if df.empty:
                st.info("Cadastre uma palestra primeiro para poder alterá-la.")
                st.stop()
                
            palestra_ids = df['ID'].unique().tolist()
            update_id = st.selectbox("ID da Palestra a Alterar", palestra_ids, key="upd_palestra_id")
            
            dados = {}
            if update_id:
                col, data_atual = buscar_palestra_por_id(update_id)
                if data_atual and len(data_atual) > 0:
                    dados = dict(zip(col, data_atual[0]))
                    evento_atual_nome = next((nome for nome, id in eventos_map.items() if id == dados['EVENTO_ID']), eventos_list[0])
                    palestrante_atual_nome = next((nome for nome, id in palestrantes_map.items() if id == dados['PALESTRANTE_ID']), palestrantes_list[0])
                else:
                    st.warning("Não foi possível carregar os dados atuais.")

            c1, c2 = st.columns(2)
            upd_titulo = c1.text_input("Novo Título", value=dados.get('TITULO', ''))
            upd_sala = c2.text_input("Nova Sala/Local", value=dados.get('SALA', ''))

            upd_descricao = st.text_area("Nova Descrição", value=dados.get('DESCRICAO', ''))
            
            c3, c4 = st.columns(2)
            data_val = dados.get('DATA')
            upd_data = c3.date_input("Nova Data", value=data_val if data_val else datetime.date.today())
            
            hora_val = dados.get('HORA')
            upd_hora = c4.time_input("Nova Hora", value=hora_val if hora_val else datetime.time(10, 0))
            
            c5, c6 = st.columns(2)
            upd_nome_evento = c5.selectbox("Novo Evento", eventos_list, index=eventos_list.index(evento_atual_nome) if 'evento_atual_nome' in locals() and evento_atual_nome in eventos_list else 0)
            upd_nome_palestrante = c6.selectbox("Novo Palestrante", palestrantes_list, index=palestrantes_list.index(palestrante_atual_nome) if 'palestrante_atual_nome' in locals() and palestrante_atual_nome in palestrantes_list else 0)

            upd_button = st.form_submit_button("Atualizar Palestra")

            if upd_button:
                upd_evento_id = eventos_map[upd_nome_evento]
                upd_palestrante_id = palestrantes_map[upd_nome_palestrante]
                upd_hora_str = upd_hora.strftime('%H:%M:%S')
                resultado = atualizar_palestra(update_id, upd_titulo, upd_descricao, str(upd_data), upd_hora_str, upd_sala, upd_evento_id, upd_palestrante_id)
                
                if isinstance(resultado, int) and resultado > 0:
                    st.success(f"Palestra ID {update_id} atualizada com sucesso.")
                    st.experimental_rerun()
                elif isinstance(resultado, str):
                    st.error(f"Erro ao atualizar: {resultado}")
                else:
                    st.warning("Nenhuma alteração realizada ou erro desconhecido.")

    # --- DELETE ---
    with tab3:
        with st.form("form_palestra_delete"):
            st.subheader("Excluir Palestra")
            if df.empty:
                st.info("Nenhuma palestra para excluir.")
                st.stop()
            
            delete_id = st.selectbox("ID da Palestra a Excluir", df['ID'].unique().tolist(), key="del_palestra_id_tab")
            del_button = st.form_submit_button("Confirmar Exclusão")

            if del_button:
                resultado = deletar_palestra(delete_id)
                if isinstance(resultado, int) and resultado > 0:
                    st.success(f"Palestra ID {delete_id} excluída com sucesso.")
                    st.experimental_rerun()
                elif isinstance(resultado, str):
                    st.error(f"Erro ao excluir. Verifique se há inscrições (INSCRICOES) ligadas a esta Palestra.")
                else:
                    st.warning("Palestra não encontrada.")


def mostrar_crud_inscricoes():
    st.header("4. 📝 Inscrição e Matrícula em Palestras")
    
    _, dados_participantes = buscar_participantes()
    _, dados_palestras = buscar_todas_palestras()

    participantes_map = {nome: id for id, nome in dados_participantes} if dados_participantes else {}
    palestras_map = {titulo: id for id, titulo in dados_palestras} if dados_palestras else {}
    participantes_list = list(participantes_map.keys())
    palestras_list = list(palestras_map.keys())

    colunas, dados = ler_inscricoes()
    
    if dados and len(dados) > 0:
        df = pd.DataFrame(dados, columns=colunas)
        st.dataframe(df.drop(columns=['PARTICIPANTE_ID', 'PALESTRA_ID']), use_container_width=True) 
    else:
        st.info("Nenhuma inscrição cadastrada.")
        df = pd.DataFrame()

    st.divider()

    if not participantes_list or not palestras_list:
        st.error("🚨 É necessário ter Participantes e Palestras cadastradas para realizar uma inscrição.")
        return

    tab1, tab2 = st.tabs(["➕ Inscrever", "❌ Cancelar Inscrição"])

    # --- CREATE ---
    with tab1:
        with st.form("form_inscricao_create"):
            st.subheader("Nova Inscrição")
            nome_participante = st.selectbox("Participante", participantes_list)
            titulo_palestra = st.selectbox("Palestra", palestras_list)
            data_inscricao = st.date_input("Data da Inscrição", value=datetime.date.today())
            submit_button = st.form_submit_button("Confirmar Inscrição")

            if submit_button:
                participante_id = participantes_map[nome_participante]
                palestra_id = palestras_map[titulo_palestra]
                
                resultado = criar_inscricao(participante_id, palestra_id, str(data_inscricao))
                
                if isinstance(resultado, int) and resultado > 0:
                    st.success(f"{nome_participante} inscrito na palestra '{titulo_palestra}' com sucesso!")
                    st.experimental_rerun()
                elif isinstance(resultado, str):
                    st.error(f"Erro ao inscrever: {resultado}")
                else:
                    st.warning("Erro desconhecido.")
    
    # --- DELETE ---
    with tab2:
        with st.form("form_inscricao_delete"):
            st.subheader("Cancelar Inscrição")
            
            if df.empty:
                st.info("Nenhuma inscrição ativa para cancelar.")
                st.stop()
            
            chaves_inscricoes = df.apply(lambda row: f"{row['PARTICIPANTE']} em {row['PALESTRA']}", axis=1).tolist()
            selecao_cancelar = st.selectbox("Inscrição a Cancelar", chaves_inscricoes)

            del_button = st.form_submit_button("Confirmar Cancelamento")

            if del_button:
                participante_nome = selecao_cancelar.split(" em ")[0]
                palestra_titulo = selecao_cancelar.split(" em ")[1]
                
                participante_id = participantes_map[participante_nome]
                palestra_id = palestras_map[palestra_titulo]
                
                resultado = deletar_inscricao(participante_id, palestra_id)
                
                if isinstance(resultado, int) and resultado > 0:
                    st.success(f"Inscrição de {participante_nome} em {palestra_titulo} cancelada com sucesso.")
                    st.experimental_rerun()
                elif isinstance(resultado, str):
                    st.error(f"Erro ao cancelar: {resultado}")
                else:
                    st.warning("Inscrição não encontrada.")


def mostrar_crud_pagamentos():
    st.header("5. 💲 Gestão de Transações e Pagamentos")
    
    _, dados_participantes = buscar_participantes()
    _, dados_eventos = buscar_eventos()
    
    participantes_map = {nome: id for id, nome in dados_participantes} if dados_participantes else {}
    eventos_map = {nome: id for id, nome in dados_eventos} if dados_eventos else {}
    participantes_list = list(participantes_map.keys())
    eventos_list = list(eventos_map.keys())
    status_list = ['Pendente', 'Concluído', 'Cancelado']

    colunas, dados = ler_pagamentos()
    if dados and len(dados) > 0:
        df = pd.DataFrame(dados, columns=colunas)
        st.dataframe(df, use_container_width=True)
    else:
        st.info("Nenhum pagamento cadastrado.")
        df = pd.DataFrame()
        
    st.divider()

    if not participantes_list or not eventos_list:
        st.error("🚨 É necessário ter Participantes e Eventos cadastrados.")
        return

    tab1, tab2, tab3 = st.tabs(["➕ Registrar", "✏️ Alterar", "❌ Excluir"])

    # --- CREATE ---
    with tab1:
        with st.form("form_pagamento_create"):
            st.subheader("Registrar Novo Pagamento")
            nome_participante = st.selectbox("Participante", participantes_list, key="p_part_c")
            nome_evento = st.selectbox("Evento", eventos_list, key="p_event_c")
            valor = st.number_input("Valor (R$)", min_value=0.01, format="%.2f")
            status = st.selectbox("Status", status_list)
            
            submit_button = st.form_submit_button("Registrar Pagamento")

            if submit_button:
                participante_id = participantes_map[nome_participante]
                evento_id = eventos_map[nome_evento]
                
                resultado = criar_pagamento(participante_id, evento_id, valor, status)
                
                if isinstance(resultado, int) and resultado > 0:
                    st.success(f"Pagamento de R$ {valor:.2f} registrado com sucesso!")
                    st.experimental_rerun()
                elif isinstance(resultado, str):
                    st.error(f"Erro ao registrar: {resultado}")
                else:
                    st.warning("Erro desconhecido.")

    # --- UPDATE ---
    with tab2:
        with st.form("form_pagamento_update"):
            st.subheader("Alterar Pagamento")
            if df.empty: st.info("Nenhum pagamento para alterar."); st.stop()
            
            pagamento_ids = df['ID'].unique().tolist()
            update_id = st.selectbox("ID do Pagamento a Alterar", pagamento_ids, key="p_id_upd")
            
            col, data_atual = buscar_pagamento_por_id(update_id)
            dados = dict(zip(col, data_atual[0])) if data_atual and len(data_atual) > 0 else {}
            
            part_atual_nome = next((n for n, id in participantes_map.items() if id == dados.get('PARTICIPANTE_ID')), participantes_list[0])
            event_atual_nome = next((n for n, id in eventos_map.items() if id == dados.get('EVENTO_ID')), eventos_list[0])

            upd_nome_participante = st.selectbox("Novo Participante", participantes_list, index=participantes_list.index(part_atual_nome), key="p_part_upd")
            upd_nome_evento = st.selectbox("Novo Evento", eventos_list, index=eventos_list.index(event_atual_nome), key="p_event_upd")
            upd_valor = st.number_input("Novo Valor (R$)", value=dados.get('VALOR', 0.01), min_value=0.01, format="%.2f")
            upd_status = st.selectbox("Novo Status", status_list, index=status_list.index(dados.get('STATUS')) if dados.get('STATUS') in status_list else 0)
            
            upd_button = st.form_submit_button("Atualizar Pagamento")

            if upd_button:
                upd_participante_id = participantes_map[upd_nome_participante]
                upd_evento_id = eventos_map[upd_nome_evento]
                
                resultado = atualizar_pagamento(update_id, upd_participante_id, upd_evento_id, upd_valor, upd_status)
                
                if isinstance(resultado, int) and resultado > 0:
                    st.success(f"Pagamento ID {update_id} atualizado.")
                    st.experimental_rerun()
                elif isinstance(resultado, str):
                    st.error(f"Erro ao atualizar: {resultado}")
                else:
                    st.warning("Nenhuma alteração realizada.")
    
    # --- DELETE ---
    with tab3:
        with st.form("form_pagamento_delete"):
            st.subheader("Excluir Pagamento")
            if df.empty: st.info("Nenhum pagamento para excluir."); st.stop()
            
            delete_id = st.selectbox("ID do Pagamento a Excluir", df['ID'].unique().tolist(), key="p_id_del")
            del_button = st.form_submit_button("Excluir Pagamento")

            if del_button:
                resultado = deletar_pagamento(delete_id)
                if isinstance(resultado, int) and resultado > 0:
                    st.success(f"Pagamento ID {delete_id} excluído.")
                    st.experimental_rerun()
                elif isinstance(resultado, str):
                    st.error(f"Erro ao excluir: {resultado}")


def mostrar_crud_tipos_pagamento():
    st.header("6. 🏷️ Definição de Tipos de Pagamento")
    
    colunas, dados = ler_tipos_pagamento()
    if dados and len(dados) > 0:
        df = pd.DataFrame(dados, columns=colunas)
        st.dataframe(df, use_container_width=True)
    else:
        st.info("Nenhum tipo de pagamento cadastrado.")
        df = pd.DataFrame()

    tab1, tab2, tab3 = st.tabs(["➕ Adicionar", "✏️ Alterar", "❌ Excluir"])

    # --- CREATE ---
    with tab1:
        with st.form("form_tipo_pagamento_create"):
            st.subheader("Novo Tipo (Ex: Cartão, Pix)")
            nome = st.text_input("Nome do Tipo")
            submit_button = st.form_submit_button("Adicionar Tipo")
            if submit_button:
                resultado = criar_tipo_pagamento(nome)
                if isinstance(resultado, int) and resultado > 0:
                    st.success(f"Tipo '{nome}' cadastrado.")
                    st.experimental_rerun()
                elif isinstance(resultado, str):
                    st.error(f"Erro: {resultado}")

    # --- UPDATE ---
    with tab2:
        with st.form("form_tipo_pagamento_update"):
            st.subheader("Alterar Tipo")
            if df.empty: st.info("Nenhum tipo para alterar."); st.stop()
            upd_id = st.selectbox("ID do Tipo a Alterar", df['ID'].unique(), key="upd_tipo_id")
            upd_nome = st.text_input("Novo Nome do Tipo")
            upd_button = st.form_submit_button("Alterar Tipo")
            if upd_button:
                resultado = atualizar_tipo_pagamento(upd_id, upd_nome)
                if isinstance(resultado, int) and resultado > 0:
                    st.success(f"Tipo ID {upd_id} atualizado para '{upd_nome}'.")
                    st.experimental_rerun()
                elif isinstance(resultado, str):
                    st.error(f"Erro: {resultado}")

    # --- DELETE ---
    with tab3:
        with st.form("form_tipo_pagamento_delete"):
            st.subheader("Excluir Tipo")
            if df.empty: st.info("Nenhum tipo para excluir."); st.stop()
            delete_id = st.selectbox("ID do Tipo a Excluir", df['ID'].unique(), key="del_tipo_id")
            del_button = st.form_submit_button("Excluir Tipo")
            if del_button:
                resultado = deletar_tipo_pagamento(delete_id)
                if isinstance(resultado, int) and resultado > 0:
                    st.success(f"Tipo ID {delete_id} excluído.")
                    st.experimental_rerun()
                elif isinstance(resultado, str):
                    st.error(f"Erro ao excluir. Verifique se há Pagamentos ligados.")


def mostrar_crud_feedback():
    st.header("7. 💬 Coleta e Análise de Feedback")
    
    _, dados_participantes = buscar_participantes()
    _, dados_palestras = buscar_todas_palestras()
    participantes_map = {nome: id for id, nome in dados_participantes} if dados_participantes else {}
    palestras_map = {titulo: id for id, titulo in dados_palestras} if dados_palestras else {}
    participantes_list = list(participantes_map.keys())
    palestras_list = list(palestras_map.keys())

    colunas, dados = ler_feedback()
    if dados and len(dados) > 0:
        df = pd.DataFrame(dados, columns=colunas)
        st.dataframe(df, use_container_width=True)
    else:
        st.info("Nenhum feedback cadastrado.")
        df = pd.DataFrame()
    st.divider()

    if not participantes_list or not palestras_list:
        st.error("🚨 É necessário ter Participantes e Palestras cadastradas.")
        return

    tab1, tab2, tab3 = st.tabs(["➕ Registrar", "✏️ Alterar", "❌ Excluir"])

    # --- CREATE ---
    with tab1:
        with st.form("form_feedback_create"):
            st.subheader("Novo Feedback")
            nome_participante = st.selectbox("Participante", participantes_list, key="f_part_c")
            titulo_palestra = st.selectbox("Palestra", palestras_list, key="f_pal_c")
            nota = st.slider("Nota (0-5)", 0, 5, 5)
            comentario = st.text_area("Comentário")
            submit_button = st.form_submit_button("Registrar Feedback")

            if submit_button:
                part_id = participantes_map[nome_participante]
                pal_id = palestras_map[titulo_palestra]
                resultado = upsert_feedback(part_id, pal_id, nota, comentario)
                if isinstance(resultado, int) and resultado > 0:
                    st.success(f"Feedback registrado para '{titulo_palestra}'.")
                    st.experimental_rerun()
                elif isinstance(resultado, str):
                    st.error(f"Erro: {resultado}")

    if not df.empty:
        with tab2:
            with st.form("form_feedback_update"):
                st.subheader("Alterar Feedback")
                feedback_ids = df['ID'].unique().tolist()
                update_id = st.selectbox("ID do Feedback a Alterar", feedback_ids, key="f_id_upd")
                
                upd_nota = st.slider("Nova Nota (0-5)", 0, 5, 5, key="f_nota_upd")
                upd_button = st.form_submit_button("Atualizar Feedback")

                if upd_button:
                    col, data = buscar_feedback_por_id(update_id)
                    if data:
                        dados = dict(zip(col, data[0]))
                        resultado = atualizar_feedback(update_id, dados['PARTICIPANTE_ID'], dados['PALESTRA_ID'], upd_nota, dados['COMENTARIO'])
                        if isinstance(resultado, int) and resultado > 0:
                            st.success(f"Feedback ID {update_id} atualizado.")
                            st.experimental_rerun()
                        elif isinstance(resultado, str):
                            st.error(f"Erro ao atualizar: {resultado}")
        
        # DELETE
        with tab3:
            with st.form("form_feedback_delete"):
                st.subheader("Excluir Feedback")
                delete_id = st.selectbox("ID do Feedback a Excluir", df['ID'].unique().tolist(), key="f_id_del")
                del_button = st.form_submit_button("Excluir Feedback")

                if del_button:
                    resultado = deletar_feedback(delete_id)
                    if isinstance(resultado, int) and resultado > 0:
                        st.success(f"Feedback ID {delete_id} excluído.")
                        st.experimental_rerun()
                    elif isinstance(resultado, str):
                        st.error(f"Erro ao excluir: {resultado}")


def mostrar_consultas():
    st.header("8. 📊 Relatórios e Consultas Complexas (Fase 3/4)")
    
    # --- Consulta 1 (Fase 3: 3+ Tabelas) ---
    st.subheader("1. Participantes por Palestra e Evento (Fase 3)")
    st.caption("Requisito: Consulta envolvendo pelo menos 3 tabelas (Inscricoes, Pessoas, Palestras, Eventos).")
    colunas, dados = consulta_participantes_palestra()
    if dados and colunas:
        df = pd.DataFrame(dados, columns=colunas)
        st.dataframe(df, use_container_width=True)
    else:
        st.info("Nenhuma inscrição encontrada ou erro.")

    st.markdown("---")
    st.title("Consultas Fase 4")

    st.subheader("2. Pessoas NÃO Inscritas em um Evento Específico")
    st.caption("Requisito: Consulta com SELECT aninhado (Participantes sem NENHUMA inscrição em um Evento).")
    
    _, dados_eventos = buscar_eventos()
    eventos_map = {nome: id for id, nome in dados_eventos} if dados_eventos else {}
    eventos_list = list(eventos_map.keys())

    if eventos_list:
        nome_evento_sel = st.selectbox("Selecione o Evento de Referência", eventos_list, key="aninhada_1_event")
        evento_id_ref = eventos_map[nome_evento_sel]

        colunas, dados = consulta_aninhada_1_nao_inscritos_em_evento_x(evento_id_ref)
        if dados and colunas:
            df = pd.DataFrame(dados, columns=colunas)
            st.dataframe(df, use_container_width=True)
        else:
            st.info("Todos os participantes estão inscritos ou não há dados.")
    else:
        st.warning("Cadastre eventos primeiro para rodar esta consulta.")

    st.markdown("---")

    st.subheader("3. Palestras com Nota Média Acima da Média Geral")
    st.caption("Requisito: Consulta com SELECT aninhado (Compara média da palestra com média global).")
    colunas, dados = consulta_aninhada_2_palestras_acima_media()
    if dados and colunas:
        df = pd.DataFrame(dados, columns=colunas)
        st.dataframe(df, use_container_width=True)
    else:
        st.info("Não há feedbacks suficientes para esta análise.")

    st.markdown("---")

    st.subheader("4. Produtividade e Arrecadação por Organizador")
    st.caption("Requisito: Consulta com função de grupo (COUNT e SUM, com GROUP BY e HAVING).")
    colunas, dados = consulta_grupo_1_total_eventos_por_organizador()
    if dados and colunas:
        df = pd.DataFrame(dados, columns=colunas)
        st.dataframe(df, use_container_width=True)
    else:
        st.info("Nenhum organizador encontrado ou sem eventos/pagamentos.")

    st.markdown("---")

    st.subheader("5. Estatísticas de Pagamento por Status")
    st.caption("Requisito: Consulta com função de grupo (AVG, MAX, MIN).")
    colunas, dados = consulta_grupo_2_estatisticas_por_status_pagamento()
    if dados and colunas:
        df = pd.DataFrame(dados, columns=colunas)
        st.dataframe(df, use_container_width=True)
    else:
        st.info("Nenhum pagamento registrado.")

    st.markdown("---")

    st.subheader("6. Lista Consolidada de Atores Financeiros")
    st.caption("Requisito: Consulta com operador de conjunto (UNION).")
    colunas, dados = consulta_conjunto_1_atores_financeiros()
    if dados and colunas:
        df = pd.DataFrame(dados, columns=colunas)
        st.dataframe(df, use_container_width=True)
    else:
        st.info("Nenhum dado financeiro ou de organização encontrado.")

    st.markdown("---")

    st.subheader("7. Palestras com Inscrição, mas Sem Feedback")
    st.caption("Requisito: Consulta com operador de conjunto (EXCEPT/MINUS).")
    colunas, dados = consulta_conjunto_2_palestras_sem_feedback()
    if dados and colunas:
        df = pd.DataFrame(dados, columns=colunas)
        st.dataframe(df, use_container_width=True)
    else:
        st.info("Todas as palestras com inscrições têm feedback, ou não há dados.")

# --- Gerenciamento de Estado da Navegação ---
if 'page' not in st.session_state:
    st.session_state.page = 'Home'

# --- Configuração do Menu Lateral ---
st.sidebar.title("✨ Plataforma bora.ai")
st.sidebar.markdown("---")

MODULOS = {
    "Home": "🏠 Início",
    "Pessoas": "👥 Gerenciamento de Usuários",
    "Eventos": "📋 Manutenção de Eventos",
    "Palestras": "🎤 Gerenciamento de Palestras",
    "Inscricoes": "📝 Controle de Inscrições",
    "Pagamentos": "💲 Manutenção Financeira",
    "Tipos_Pagamento": "🏷️ Tipos de Pagamento (Suporte)",
    "Feedback": "💬 Análise de Feedback",
    "Consultas": "📊 Relatórios e Análise (Fase 4)"
}

st.sidebar.subheader("Navegação Principal")
for key, value in MODULOS.items():
    if st.sidebar.button(value, key=f"nav_btn_{key}"):
        st.session_state.page = key

st.sidebar.markdown("---")
st.sidebar.caption("Exercício-Programa 2/25 - USP EACH")

# --- Função Principal de Roteamento ---

def router():
    """Direciona o usuário para a tela selecionada no menu lateral."""
    page = st.session_state.page

    if page == 'Home':
        st.title("Bem-vindo à Plataforma de Gerenciamento de Eventos")
        st.info("Utilize o menu lateral para acessar as funcionalidades de Manutenção e Relatórios.")
        st.markdown(f"**Status da Entrega (Fase 3):** 7/7 tabelas implementadas.")
        st.markdown(f"**Status da Entrega (Fase 4):** 6/6 consultas complexas implementadas na tela 'Relatórios e Análise'.")

    elif page == 'Pessoas':
        mostrar_crud_pessoas()
    elif page == 'Eventos':
        mostrar_crud_eventos()
    elif page == 'Palestras':
        mostrar_crud_palestras()
    elif page == 'Inscricoes':
        mostrar_crud_inscricoes()
    elif page == 'Pagamentos':
        mostrar_crud_pagamentos()
    elif page == 'Tipos_Pagamento':
        mostrar_crud_tipos_pagamento()
    elif page == 'Feedback':
        mostrar_crud_feedback()
        
    elif page == 'Consultas':
        mostrar_consultas()

router()