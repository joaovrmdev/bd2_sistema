import streamlit as st
import pandas as pd
from datetime import date
from db_utils import ( 
    get_snowpark_session, buscar_ids_nomes, buscar_registro_por_id, deletar_registro_por_id,
    
    # CRUD PESSOAS
    criar_pessoa, ler_pessoas, atualizar_pessoa,
    
    # CRUD EVENTOS
    criar_evento, ler_eventos, atualizar_evento,
    
    # CRUD PALESTRAS
    criar_palestra, ler_palestras, atualizar_palestra,
    
    # CRUD INSCRICOES
    criar_inscricao, ler_inscricoes, deletar_inscricao,
    
    # CRUD PAGAMENTOS
    criar_pagamento, ler_pagamentos, atualizar_pagamento,
    
    # CRUD TIPOS_PAGAMENTO
    criar_tipo_pagamento, ler_tipos_pagamento, atualizar_tipo_pagamento,
    
    # CRUD FEEDBACK
    upsert_feedback, ler_feedback, atualizar_feedback,
    
    # CONSULTAS (FASE 3/4)
    consulta_participantes_palestra,
    consulta_aninhada_1_nao_inscritos_em_evento_x,
    consulta_aninhada_2_palestras_acima_media,
    consulta_grupo_1_total_eventos_por_organizador,
    consulta_grupo_2_estatisticas_por_status_pagamento,
    consulta_conjunto_1_atores_financeiros,
    consulta_conjunto_2_palestras_sem_feedback
)

# --- CONFIGURAÇÃO INICIAL E SESSÃO SNOWPARK ---

st.set_page_config(layout="wide", page_title="Plataforma bora.ai")

# Conecta e armazena a sessão Snowpark
snowpark_session = get_snowpark_session()

# --- FUNÇÕES DE INTERFACE (MANUTENÇÃO) ---

def mostrar_crud_pessoas():
    st.header("1. 👥 Cadastro e Manutenção de Usuários")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Cadastrar Novo Usuário")
        with st.form("form_pessoa_create"):
            nome = st.text_input("Nome")
            email = st.text_input("Email", key="p_email_c")
            telefone = st.text_input("Telefone")
            tipo = st.selectbox("Tipo de Pessoa", ['Participante', 'Organizador', 'Palestrante'])
            
            submit_button = st.form_submit_button("Cadastrar")

            if submit_button:
                resultado = criar_pessoa(snowpark_session, nome, email, telefone, tipo)
                if isinstance(resultado, int) and resultado > 0:
                    st.success("Usuário cadastrado com sucesso!")
                    st.experimental_rerun()
                else:
                    st.error(f"Erro ao cadastrar: {resultado}")

    with col2:
        st.subheader("Atualizar / Excluir Usuário")
        _, dados = ler_pessoas(snowpark_session)
        if not dados:
            st.info("Nenhum usuário encontrado.")
            return

        df = pd.DataFrame(dados, columns=["ID", "NOME", "EMAIL", "TELEFONE", "TIPO_PESSOA"])
        
        nomes_id_map = {f"{row['NOME']} ({row['ID']})": row['ID'] for index, row in df.iterrows()}
        selecao = st.selectbox("Selecione o Usuário para Alterar/Excluir", list(nomes_id_map.keys()), key="p_select")
        
        if selecao:
            pessoa_id = nomes_id_map[selecao]
            dados = buscar_registro_por_id(snowpark_session, 'PESSOAS', pessoa_id)

            if dados:
                upd_nome = st.text_input("Novo Nome", value=dados[1], key="p_nome_upd")
                upd_email = st.text_input("Novo Email", value=dados[2], key="p_email_upd")
                upd_telefone = st.text_input("Novo Telefone", value=dados[3] if dados[3] else "", key="p_tel_upd")
                upd_tipo = st.selectbox("Novo Tipo", ['Participante', 'Organizador', 'Palestrante'], index=['Participante', 'Organizador', 'Palestrante'].index(dados[4]), key="p_tipo_upd")

                col_upd, col_del = st.columns(2)
                with col_upd:
                    if st.button("Atualizar Usuário", key="p_upd_btn"):
                        resultado = atualizar_pessoa(snowpark_session, pessoa_id, upd_nome, upd_email, upd_telefone, upd_tipo)
                        if isinstance(resultado, int) and resultado > 0:
                            st.success("Usuário atualizado com sucesso!")
                            st.experimental_rerun()
                        else:
                            st.error(f"Erro ao atualizar: {resultado}")

                with col_del:
                    if st.button("Excluir Usuário", key="p_del_btn"):
                        resultado = deletar_registro_por_id(snowpark_session, 'PESSOAS', pessoa_id)
                        if isinstance(resultado, int) and resultado > 0:
                            st.success("Usuário excluído com sucesso!")
                            st.experimental_rerun()
                        else:
                            st.error(f"Erro ao excluir: {resultado}")
    
    st.subheader("Dados Atuais")
    colunas, dados = ler_pessoas(snowpark_session)
    if dados:
        df_read = pd.DataFrame(dados, columns=colunas)
        st.dataframe(df_read, use_container_width=True)

def mostrar_crud_eventos():
    st.header("2. 📋 Manutenção de Eventos")

    _, dados_organizadores = buscar_ids_nomes(snowpark_session, 'PESSOAS', nome_coluna='NOME')
    organizadores_map = {nome: id for id, nome in dados_organizadores if 'ORGANIZADOR' in buscar_registro_por_id(snowpark_session, 'PESSOAS', id)[4].upper() or 'PALESTRANTE' in buscar_registro_por_id(snowpark_session, 'PESSOAS', id)[4].upper()}
    organizadores_list = list(organizadores_map.keys())

    if not organizadores_list:
        st.warning("⚠️ Cadastre pelo menos um 'Organizador' ou 'Palestrante' na seção 'Usuários' para criar eventos.")
        return

    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Cadastrar Novo Evento")
        with st.form("form_evento_create"):
            nome = st.text_input("Nome do Evento")
            data_inicio = st.date_input("Data de Início", value=date.today())
            data_fim = st.date_input("Data de Fim", value=date.today())
            local = st.text_input("Local")
            nome_organizador = st.selectbox("Organizador Responsável", organizadores_list)
            
            submit_button = st.form_submit_button("Cadastrar")

            if submit_button:
                organizador_id = organizadores_map[nome_organizador]
                resultado = criar_evento(snowpark_session, nome, str(data_inicio), str(data_fim), local, organizador_id)
                if isinstance(resultado, int) and resultado > 0:
                    st.success("Evento cadastrado com sucesso!")
                    st.experimental_rerun()
                else:
                    st.error(f"Erro ao cadastrar: {resultado}")

    with col2:
        st.subheader("Atualizar / Excluir Evento")
        colunas, dados = ler_eventos(snowpark_session)
        if not dados:
            st.info("Nenhum evento encontrado.")
            return

        df = pd.DataFrame(dados, columns=colunas)
        
        eventos_map = {f"{row['NOME_EVENTO']} ({row['ID']})": row['ID'] for index, row in df.iterrows()}
        selecao = st.selectbox("Selecione o Evento para Alterar/Excluir", list(eventos_map.keys()), key="e_select")
        
        if selecao:
            evento_id = eventos_map[selecao]
            dados = buscar_registro_por_id(snowpark_session, 'EVENTOS', evento_id)

            if dados:
                upd_nome = st.text_input("Novo Nome", value=dados[1], key="e_nome_upd")
                upd_data_inicio = st.date_input("Nova Data de Início", value=pd.to_datetime(dados[2]), key="e_data_i_upd")
                upd_data_fim = st.date_input("Nova Data de Fim", value=pd.to_datetime(dados[3]) if dados[3] else upd_data_inicio, key="e_data_f_upd")
                upd_local = st.text_input("Novo Local", value=dados[4] if dados[4] else "", key="e_local_upd")
                
                organizador_atual_nome = [nome for nome, id in organizadores_map.items() if id == dados[5]][0]
                upd_nome_organizador = st.selectbox("Novo Organizador", organizadores_list, index=organizadores_list.index(organizador_atual_nome), key="e_org_upd")
                upd_organizador_id = organizadores_map[upd_nome_organizador]

                col_upd, col_del = st.columns(2)
                with col_upd:
                    if st.button("Atualizar Evento", key="e_upd_btn"):
                        resultado = atualizar_evento(snowpark_session, evento_id, upd_nome, str(upd_data_inicio), str(upd_data_fim), upd_local, upd_organizador_id)
                        if isinstance(resultado, int) and resultado > 0:
                            st.success("Evento atualizado com sucesso!")
                            st.experimental_rerun()
                        else:
                            st.error(f"Erro ao atualizar: {resultado}")

                with col_del:
                    if st.button("Excluir Evento", key="e_del_btn"):
                        resultado = deletar_registro_por_id(snowpark_session, 'EVENTOS', evento_id)
                        if isinstance(resultado, int) and resultado > 0:
                            st.success("Evento excluído com sucesso!")
                            st.experimental_rerun()
                        else:
                            st.error(f"Erro ao excluir: {resultado}")

    st.subheader("Dados Atuais")
    colunas, dados = ler_eventos(snowpark_session)
    if dados:
        df_read = pd.DataFrame(dados, columns=[c.upper() for c in colunas])
        st.dataframe(df_read, use_container_width=True)


def mostrar_crud_palestras():
    st.header("3. 🎤 Cadastro de Palestras")

    # Obter Eventos e Palestrantes
    _, dados_eventos = buscar_ids_nomes(snowpark_session, 'EVENTOS')
    _, dados_palestrantes = buscar_ids_nomes(snowpark_session, 'PESSOAS', nome_coluna='NOME')
    
    eventos_map = {nome: id for id, nome in dados_eventos}
    eventos_list = list(eventos_map.keys())

    # Filtra apenas quem é 'Palestrante' ou 'Organizador' para dar palestras
    palestrantes_map = {nome: id for id, nome in dados_palestrantes if 'PALESTRANTE' in buscar_registro_por_id(snowpark_session, 'PESSOAS', id)[4].upper() or 'ORGANIZADOR' in buscar_registro_por_id(snowpark_session, 'PESSOAS', id)[4].upper()}
    palestrantes_list = list(palestrantes_map.keys())


    if not eventos_list or not palestrantes_list:
        st.warning("⚠️ Cadastre pelo menos um 'Evento' e um 'Palestrante' na seção 'Usuários' para criar palestras.")
        return

    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Cadastrar Nova Palestra")
        with st.form("form_palestra_create"):
            titulo = st.text_input("Título")
            descricao = st.text_area("Descrição")
            data = st.date_input("Data", value=date.today())
            hora = st.time_input("Hora", value=pd.to_datetime('10:00:00').time())
            sala = st.text_input("Sala")
            nome_evento = st.selectbox("Evento", eventos_list, key="p_evento_c")
            nome_palestrante = st.selectbox("Palestrante", palestrantes_list, key="p_palestrante_c")
            
            submit_button = st.form_submit_button("Cadastrar")

            if submit_button:
                evento_id = eventos_map[nome_evento]
                palestrante_id = palestrantes_map[nome_palestrante]
                hora_str = hora.strftime('%H:%M:%S') 
                
                resultado = criar_palestra(snowpark_session, titulo, descricao, str(data), hora_str, sala, evento_id, palestrante_id)
                if isinstance(resultado, int) and resultado > 0:
                    st.success("Palestra cadastrada com sucesso!")
                    st.experimental_rerun()
                else:
                    st.error(f"Erro ao cadastrar: {resultado}")

    with col2:
        st.subheader("Atualizar / Excluir Palestra")
        colunas, dados = ler_palestras(snowpark_session)
        if not dados:
            st.info("Nenhuma palestra encontrada.")
            return

        df = pd.DataFrame(dados, columns=colunas)
        
        palestras_map = {f"{row['TITULO']} - {row['EVENTO']}": row['ID'] for index, row in df.iterrows()}
        selecao = st.selectbox("Selecione a Palestra para Alterar/Excluir", list(palestras_map.keys()), key="p_select")
        
        if selecao:
            update_id = palestras_map[selecao]
            dados = buscar_registro_por_id(snowpark_session, 'PALESTRAS', update_id)

            if dados:
                upd_titulo = st.text_input("Novo Título", value=dados[1], key="p_titulo_upd")
                upd_descricao = st.text_area("Nova Descrição", value=dados[2] if dados[2] else "", key="p_desc_upd")
                upd_data = st.date_input("Nova Data", value=pd.to_datetime(dados[3]), key="p_data_upd")
                upd_hora = st.time_input("Nova Hora", value=pd.to_datetime(str(dados[4])).time(), key="p_hora_upd")
                upd_sala = st.text_input("Nova Sala", value=dados[5] if dados[5] else "", key="p_sala_upd")

                evento_atual_nome = [nome for nome, id in eventos_map.items() if id == dados[6]][0]
                upd_nome_evento = st.selectbox("Novo Evento", eventos_list, index=eventos_list.index(evento_atual_nome), key="p_evento_upd")
                upd_evento_id = eventos_map[upd_nome_evento]

                palestrante_atual_nome = [nome for nome, id in palestrantes_map.items() if id == dados[7]][0]
                upd_nome_palestrante = st.selectbox("Novo Palestrante", palestrantes_list, index=palestrantes_list.index(palestrante_atual_nome), key="p_palestrante_upd")
                upd_palestrante_id = palestrantes_map[upd_nome_palestrante]

                col_upd, col_del = st.columns(2)
                with col_upd:
                    if st.button("Atualizar Palestra", key="p_upd_btn"):
                        upd_hora_str = upd_hora.strftime('%H:%M:%S')
                        resultado = atualizar_palestra(snowpark_session, update_id, upd_titulo, upd_descricao, str(upd_data), upd_hora_str, upd_sala, upd_evento_id, upd_palestrante_id)
                        if isinstance(resultado, int) and resultado > 0:
                            st.success("Palestra atualizada com sucesso!")
                            st.experimental_rerun()
                        else:
                            st.error(f"Erro ao atualizar: {resultado}")

                with col_del:
                    if st.button("Excluir Palestra", key="p_del_btn"):
                        resultado = deletar_registro_por_id(snowpark_session, 'PALESTRAS', update_id)
                        if isinstance(resultado, int) and resultado > 0:
                            st.success("Palestra excluída com sucesso!")
                            st.experimental_rerun()
                        else:
                            st.error(f"Erro ao excluir: {resultado}")

    st.subheader("Dados Atuais")
    colunas, dados = ler_palestras(snowpark_session)
    if dados:
        df_read = pd.DataFrame(dados, columns=[c.upper() for c in colunas])
        st.dataframe(df_read, use_container_width=True)

def mostrar_crud_inscricoes():
    st.header("4. 📝 Inscrição e Matrícula em Palestras")

    # Obter Participantes e Palestras
    _, dados_participantes = buscar_ids_nomes(snowpark_session, 'PESSOAS', nome_coluna='NOME')
    _, dados_palestras = buscar_ids_nomes(snowpark_session, 'PALESTRAS', nome_coluna='TITULO')
    
    participantes_map = {nome: id for id, nome in dados_participantes if 'PARTICIPANTE' in buscar_registro_por_id(snowpark_session, 'PESSOAS', id)[4].upper()}
    participantes_list = list(participantes_map.keys())
    
    palestras_map = {titulo: id for id, titulo in dados_palestras}
    palestras_list = list(palestras_map.keys())

    if not participantes_list or not palestras_list:
        st.warning("⚠️ Cadastre pelo menos um 'Participante' e uma 'Palestra' para gerenciar inscrições.")
        return

    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Registrar Nova Inscrição")
        with st.form("form_inscricao_create"):
            nome_participante = st.selectbox("Participante", participantes_list, key="i_part_c")
            titulo_palestra = st.selectbox("Palestra", palestras_list, key="i_pal_c")
            data_inscricao = st.date_input("Data da Inscrição", value=date.today())
            
            submit_button = st.form_submit_button("Inscrever")

            if submit_button:
                participante_id = participantes_map[nome_participante]
                palestra_id = palestras_map[titulo_palestra]
                
                resultado = criar_inscricao(snowpark_session, participante_id, palestra_id, str(data_inscricao))
                if isinstance(resultado, int) and resultado > 0:
                    st.success("Inscrição registrada com sucesso!")
                    st.experimental_rerun()
                else:
                    st.error(f"Erro ao inscrever (Inscrição duplicada ou erro no DB): {resultado}")

    with col2:
        st.subheader("Cancelar Inscrição")
        colunas, dados = ler_inscricoes(snowpark_session)
        if not dados:
            st.info("Nenhuma inscrição encontrada.")
            return

        df = pd.DataFrame(dados, columns=[c.upper() for c in colunas])
        
        # Correção: Usar PARTICIPANTE e PALESTRA (Upper case)
        chaves_inscricoes = df.apply(lambda row: f"{row['PARTICIPANTE']} em {row['PALESTRA']}", axis=1).tolist()
        
        selecao_cancelar = st.selectbox("Selecione a Inscrição para Cancelar", chaves_inscricoes, key="i_select_del")
        
        if selecao_cancelar:
            participante_nome = selecao_cancelar.split(" em ")[0]
            palestra_titulo = selecao_cancelar.split(" em ")[1]
            
            participante_id = participantes_map[participante_nome]
            palestra_id = palestras_map[palestra_titulo]
            
            if st.button("Confirmar Cancelamento", key="i_del_btn"):
                resultado = deletar_inscricao(snowpark_session, participante_id, palestra_id)
                if isinstance(resultado, int) and resultado > 0:
                    st.success("Inscrição cancelada com sucesso!")
                    st.experimental_rerun()
                else:
                    st.error(f"Erro ao cancelar: {resultado}")

    st.subheader("Dados Atuais")
    colunas, dados = ler_inscricoes(snowpark_session)
    if dados:
        df_read = pd.DataFrame(dados, columns=[c.upper() for c in colunas])
        st.dataframe(df_read, use_container_width=True)

def mostrar_crud_pagamentos():
    st.header("5. 💲 Gestão de Transações e Pagamentos")

    # Obter Participantes, Eventos e Tipos de Pagamento
    _, dados_participantes = buscar_ids_nomes(snowpark_session, 'PESSOAS', nome_coluna='NOME')
    participantes_map = {nome: id for id, nome in dados_participantes if 'PARTICIPANTE' in buscar_registro_por_id(snowpark_session, 'PESSOAS', id)[4].upper()}
    participantes_list = list(participantes_map.keys())

    _, dados_eventos = buscar_ids_nomes(snowpark_session, 'EVENTOS')
    eventos_map = {nome: id for id, nome in dados_eventos}
    eventos_list = list(eventos_map.keys())
    
    _, dados_tipos_pag = buscar_ids_nomes(snowpark_session, 'TIPOS_PAGAMENTO', nome_coluna='NOME')
    tipos_pag_map = {nome: id for id, nome in dados_tipos_pag}
    tipos_pag_list = list(tipos_pag_map.keys())

    if not participantes_list or not eventos_list or not tipos_pag_list:
        st.warning("⚠️ Cadastre 'Participantes', 'Eventos' e 'Tipos de Pagamento' para gerenciar pagamentos.")
        return

    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Registrar Novo Pagamento")
        with st.form("form_pagamento_create"):
            nome_participante = st.selectbox("Participante", participantes_list, key="pg_part_c")
            nome_evento = st.selectbox("Evento Referente", eventos_list, key="pg_evento_c")
            valor = st.number_input("Valor (R$)", min_value=0.01, format="%.2f", key="pg_valor_c")
            status = st.selectbox("Status", ['Pendente', 'Confirmado', 'Cancelado'], key="pg_status_c")
            tipo_pag_nome = st.selectbox("Tipo de Pagamento", tipos_pag_list, key="pg_tipo_c")
            
            submit_button = st.form_submit_button("Registrar")

            if submit_button:
                participante_id = participantes_map[nome_participante]
                evento_id = eventos_map[nome_evento]
                tipo_pagamento_id = tipos_pag_map[tipo_pag_nome]

                resultado = criar_pagamento(snowpark_session, participante_id, evento_id, valor, status, tipo_pagamento_id)
                if isinstance(resultado, int) and resultado > 0:
                    st.success("Pagamento registrado com sucesso!")
                    st.experimental_rerun()
                else:
                    st.error(f"Erro ao registrar: {resultado}")

    with col2:
        st.subheader("Atualizar / Excluir Pagamento")
        colunas, dados = ler_pagamentos(snowpark_session)
        if not dados:
            st.info("Nenhum pagamento encontrado.")
            return

        df = pd.DataFrame(dados, columns=[c.upper() for c in colunas])
        
        pagamentos_map = {f"ID {row['ID']} - R$ {row['VALOR']} ({row['PARTICIPANTE']} em {row['EVENTO']})": row['ID'] for index, row in df.iterrows()}
        selecao = st.selectbox("Selecione o Pagamento para Alterar/Excluir", list(pagamentos_map.keys()), key="pg_select")
        
        if selecao:
            update_id = pagamentos_map[selecao]
            dados_pag = buscar_registro_por_id(snowpark_session, 'PAGAMENTOS', update_id)

            if dados_pag:
                # Corrigido: Usar índice 3 (valor) e 5 (tipo_pagamento_id) da tabela PAGAMENTOS
                valor_atual_float = float(dados_pag[3])
                
                # Tratamento para NULL/Ausência de Tipo de Pagamento (resolvendo IndexError)
                tipo_pag_id_atual = dados_pag[5]
                
                if tipo_pag_id_atual is None:
                    tipo_pag_nome_atual = tipos_pag_list[0] if tipos_pag_list else ""
                else:
                    match = [nome for nome, id in tipos_pag_map.items() if id == tipo_pag_id_atual]
                    tipo_pag_nome_atual = match[0] if match else tipos_pag_list[0] if tipos_pag_list else ""
                
                # Obter participantes e eventos atuais para FKs (necessário para atualizar a FK no banco)
                participante_id_atual = dados_pag[1]
                evento_id_atual = dados_pag[2]

                upd_valor = st.number_input("Novo Valor (R$)", value=valor_atual_float, min_value=0.01, format="%.2f", key="p_valor_upd")
                
                status_atual = dados_pag[4]
                upd_status = st.selectbox("Novo Status", ['Pendente', 'Confirmado', 'Cancelado'], index=['Pendente', 'Confirmado', 'Cancelado'].index(status_atual), key="pg_status_upd")

                upd_tipo_pag_nome = st.selectbox("Novo Tipo de Pagamento", tipos_pag_list, 
                                                 index=tipos_pag_list.index(tipo_pag_nome_atual) if tipo_pag_nome_atual in tipos_pag_list else 0, 
                                                 key="pg_tipo_upd")
                upd_tipo_pagamento_id = tipos_pag_map[upd_tipo_pag_nome]

                col_upd, col_del = st.columns(2)
                with col_upd:
                    if st.button("Atualizar Pagamento", key="pg_upd_btn"):
                        # Corrigido: Passar todas as colunas UPDATE na função atualizar_pagamento
                        resultado = atualizar_pagamento(snowpark_session, update_id, upd_valor, upd_status, upd_tipo_pagamento_id)
                        if isinstance(resultado, int) and resultado > 0:
                            st.success("Pagamento atualizado com sucesso!")
                            st.experimental_rerun()
                        else:
                            st.error(f"Erro ao atualizar: {resultado}")

                with col_del:
                    if st.button("Excluir Pagamento", key="pg_del_btn"):
                        resultado = deletar_registro_por_id(snowpark_session, 'PAGAMENTOS', update_id)
                        if isinstance(resultado, int) and resultado > 0:
                            st.success("Pagamento excluído com sucesso!")
                            st.experimental_rerun()
                        else:
                            st.error(f"Erro ao excluir: {resultado}")

    st.subheader("Dados Atuais")
    colunas, dados = ler_pagamentos(snowpark_session)
    if dados:
        df_read = pd.DataFrame(dados, columns=[c.upper() for c in colunas])
        st.dataframe(df_read, use_container_width=True)

def mostrar_crud_tipos_pagamento():
    st.header("6. 🏷️ Definição de Tipos de Pagamento")

    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Cadastrar Novo Tipo")
        with st.form("form_tipo_pag_create"):
            nome = st.text_input("Nome do Tipo de Pagamento (Ex: Boleto, Pix, Cartão)")
            
            submit_button = st.form_submit_button("Cadastrar")

            if submit_button:
                resultado = criar_tipo_pagamento(snowpark_session, nome)
                if isinstance(resultado, int) and resultado > 0:
                    st.success("Tipo de Pagamento cadastrado com sucesso!")
                    st.experimental_rerun()
                else:
                    st.error(f"Erro ao cadastrar: {resultado}")

    with col2:
        st.subheader("Atualizar / Excluir Tipo")
        _, dados = ler_tipos_pagamento(snowpark_session)
        if not dados:
            st.info("Nenhum tipo de pagamento encontrado.")
            return

        df = pd.DataFrame(dados, columns=["ID", "NOME"])
        
        tipos_map = {f"{row['NOME']} ({row['ID']})": row['ID'] for index, row in df.iterrows()}
        selecao = st.selectbox("Selecione o Tipo para Alterar/Excluir", list(tipos_map.keys()), key="t_select")
        
        if selecao:
            tipo_id = tipos_map[selecao]
            dados = buscar_registro_por_id(snowpark_session, 'TIPOS_PAGAMENTO', tipo_id)

            if dados:
                upd_nome = st.text_input("Novo Nome", value=dados[1], key="t_nome_upd")

                col_upd, col_del = st.columns(2)
                with col_upd:
                    if st.button("Atualizar Tipo", key="t_upd_btn"):
                        resultado = atualizar_tipo_pagamento(snowpark_session, tipo_id, upd_nome)
                        if isinstance(resultado, int) and resultado > 0:
                            st.success("Tipo de Pagamento atualizado com sucesso!")
                            st.experimental_rerun()
                        else:
                            st.error(f"Erro ao atualizar: {resultado}")

                with col_del:
                    if st.button("Excluir Tipo", key="t_del_btn"):
                        resultado = deletar_registro_por_id(snowpark_session, 'TIPOS_PAGAMENTO', tipo_id)
                        if isinstance(resultado, int) and resultado > 0:
                            st.success("Tipo de Pagamento excluído com sucesso!")
                            st.experimental_rerun()
                        else:
                            st.error(f"Erro ao excluir (Pode estar sendo usado em PAGAMENTOS): {resultado}")

    st.subheader("Dados Atuais")
    colunas, dados = ler_tipos_pagamento(snowpark_session)
    if dados:
        df_read = pd.DataFrame(dados, columns=[c.upper() for c in colunas])
        st.dataframe(df_read, use_container_width=True)

def mostrar_crud_feedback():
    st.header("7. 💬 Coleta e Análise de Feedback")

    # Obter Participantes e Palestras
    _, dados_participantes = buscar_ids_nomes(snowpark_session, 'PESSOAS', nome_coluna='NOME')
    participantes_map = {nome: id for id, nome in dados_participantes if 'PARTICIPANTE' in buscar_registro_por_id(snowpark_session, 'PESSOAS', id)[4].upper()}
    participantes_list = list(participantes_map.keys())

    _, dados_palestras = buscar_ids_nomes(snowpark_session, 'PALESTRAS', nome_coluna='TITULO')
    palestras_map = {titulo: id for id, titulo in dados_palestras}
    palestras_list = list(palestras_map.keys())

    if not participantes_list or not palestras_list:
        st.warning("⚠️ Cadastre 'Participantes' e 'Palestras' para coletar feedback.")
        return

    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Registrar / Atualizar Feedback")
        st.caption("Se o feedback já existir, ele será atualizado automaticamente (UPSERT).")
        with st.form("form_feedback_create"):
            nome_participante = st.selectbox("Participante", participantes_list, key="f_part_c")
            titulo_palestra = st.selectbox("Palestra Avaliada", palestras_list, key="f_pal_c")
            nota = st.slider("Nota (1 = Péssimo, 5 = Excelente)", 1, 5, 5, key="f_nota_c")
            comentario = st.text_area("Comentário (Opcional)", key="f_comentario_c")
            
            submit_button = st.form_submit_button("Registrar Feedback")

            if submit_button:
                part_id = participantes_map[nome_participante]
                pal_id = palestras_map[titulo_palestra]
                
                resultado = upsert_feedback(snowpark_session, part_id, pal_id, nota, comentario)
                
                if isinstance(resultado, int) and resultado >= 0: # 0 para INSERT, 1 para UPDATE no MERGE
                    st.success(f"Feedback para '{titulo_palestra}' registrado/atualizado com sucesso!")
                    st.experimental_rerun()
                elif isinstance(resultado, str):
                    st.error(f"Erro: {resultado}")
                else:
                    st.warning("Erro desconhecido.")

    with col2:
        st.subheader("Atualizar Nota/Comentário")
        colunas, dados = ler_feedback(snowpark_session)
        if not dados:
            st.info("Nenhum feedback encontrado.")
            return

        df = pd.DataFrame(dados, columns=[c.upper() for c in colunas])
        
        feedbacks_map = {f"ID {row['ID']} - {row['PARTICIPANTE']} ({row['NOTA']})": row['ID'] for index, row in df.iterrows()}
        selecao = st.selectbox("Selecione o Feedback para Alterar/Excluir", list(feedbacks_map.keys()), key="f_select")
        
        if selecao:
            update_id = feedbacks_map[selecao]
            dados = buscar_registro_por_id(snowpark_session, 'FEEDBACK_PALESTRAS', update_id)

            if dados:
                upd_nota = st.slider("Nova Nota", 1, 5, dados[3], key="f_nota_upd")
                upd_comentario = st.text_area("Novo Comentário", value=dados[4] if dados[4] else "", key="f_comentario_upd")
                
                col_upd, col_del = st.columns(2)
                with col_upd:
                    if st.button("Atualizar Feedback", key="f_upd_btn"):
                        resultado = atualizar_feedback(snowpark_session, update_id, upd_nota, upd_comentario)
                        if isinstance(resultado, int) and resultado > 0:
                            st.success("Feedback atualizado com sucesso!")
                            st.experimental_rerun()
                        else:
                            st.error(f"Erro ao atualizar: {resultado}")

                with col_del:
                    if st.button("Excluir Feedback", key="f_del_btn"):
                        resultado = deletar_registro_por_id(snowpark_session, 'FEEDBACK_PALESTRAS', update_id)
                        if isinstance(resultado, int) and resultado > 0:
                            st.success("Feedback excluído com sucesso!")
                            st.experimental_rerun()
                        else:
                            st.error(f"Erro ao excluir: {resultado}")

    st.subheader("Dados Atuais")
    colunas, dados = ler_feedback(snowpark_session)
    if dados:
        df_read = pd.DataFrame(dados, columns=[c.upper() for c in colunas])
        st.dataframe(df_read, use_container_width=True)

def mostrar_consultas():
    st.header("8. 📊 Relatórios e Consultas Complexas (Fase 3/4)")
    
    # --- Consulta 1 (Fase 3: 3+ Tabelas) ---
    st.subheader("1. Participantes por Palestra e Evento (Fase 3)")
    st.caption("Consulta: Listagem de todas as inscrições, mostrando Participante, Palestra, Evento e Data da Inscrição.")
    colunas, dados = consulta_participantes_palestra(snowpark_session)
    if dados and colunas:
        df = pd.DataFrame(dados, columns=[c.upper() for c in colunas])
        st.dataframe(df, use_container_width=True)
    else:
        st.info("Nenhuma inscrição encontrada ou erro.")

    st.markdown("---")
    st.title("Consultas Fase 4 (Análise Avançada)")

    # --- Consulta 2 (Aninhada 1) ---
    st.subheader("2. Pessoas NÃO Inscritas em um Evento Específico")
    st.caption("Requisito: Consulta com SELECT aninhado. Identifica participantes que não se inscreveram em nenhuma palestra de um evento selecionado.")
    
    _, dados_eventos = buscar_ids_nomes(snowpark_session, 'EVENTOS')
    eventos_map = {nome: id for id, nome in dados_eventos} if dados_eventos else {}
    eventos_list = list(eventos_map.keys())

    if eventos_list:
        nome_evento_sel = st.selectbox("Selecione o Evento de Referência", eventos_list, key="aninhada_1_event")
        evento_id_ref = eventos_map[nome_evento_sel]

        colunas, dados = consulta_aninhada_1_nao_inscritos_em_evento_x(snowpark_session, evento_id_ref)
        if dados and colunas:
            df = pd.DataFrame(dados, columns=[c.upper() for c in colunas])
            st.dataframe(df, use_container_width=True)
        else:
            st.info("Todos os participantes estão inscritos no evento selecionado ou não há dados.")
    else:
        st.warning("Cadastre eventos primeiro para rodar esta consulta.")

    st.markdown("---")

    # --- Consulta 3 (Aninhada 2) ---
    st.subheader("3. Palestras com Nota Média Acima da Média Geral")
    st.caption("Requisito: Consulta com SELECT aninhado. Compara a nota média de cada palestra com a média de feedback de todas as palestras.")
    colunas, dados = consulta_aninhada_2_palestras_acima_media(snowpark_session)
    if dados and colunas:
        df = pd.DataFrame(dados, columns=[c.upper() for c in colunas])
        st.dataframe(df, use_container_width=True)
    else:
        st.info("Não há feedbacks suficientes para esta análise.")

    st.markdown("---")

    # --- Consulta 4 (Grupo 1) ---
    st.subheader("4. Produtividade e Arrecadação por Organizador")
    st.caption("Requisito: Consulta com função de grupo (COUNT e SUM, com GROUP BY e HAVING). Mostra o total de eventos organizados e o valor total arrecadado por pagamentos associados a esses eventos.")
    colunas, dados = consulta_grupo_1_total_eventos_por_organizador(snowpark_session)
    if dados and colunas:
        df = pd.DataFrame(dados, columns=[c.upper() for c in colunas])
        st.dataframe(df, use_container_width=True)
    else:
        st.info("Nenhum organizador encontrado ou sem eventos/pagamentos.")

    st.markdown("---")

    # --- Consulta 5 (Grupo 2) ---
    st.subheader("5. Estatísticas de Pagamento por Status")
    st.caption("Requisito: Consulta com função de grupo (AVG, MAX, MIN). Fornece a média, máximo e mínimo dos valores de pagamento para cada status (Confirmado, Pendente, Cancelado).")
    colunas, dados = consulta_grupo_2_estatisticas_por_status_pagamento(snowpark_session)
    if dados and colunas:
        df = pd.DataFrame(dados, columns=[c.upper() for c in colunas])
        st.dataframe(df, use_container_width=True)
    else:
        st.info("Nenhum pagamento registrado.")

    st.markdown("---")

    # --- Consulta 6 (Conjunto 1) ---
    st.subheader("6. Lista Consolidada de Atores Financeiros")
    st.caption("Requisito: Consulta com operador de conjunto (UNION). Lista todos os Organizadores E todos os Participantes que efetuaram algum pagamento.")
    colunas, dados = consulta_conjunto_1_atores_financeiros(snowpark_session)
    if dados and colunas:
        df = pd.DataFrame(dados, columns=[c.upper() for c in colunas])
        st.dataframe(df, use_container_width=True)
    else:
        st.info("Nenhum dado financeiro ou de organização encontrado.")

    st.markdown("---")

    # --- Consulta 7 (Conjunto 2) ---
    st.subheader("7. Palestras com Inscrição, mas Sem Feedback")
    st.caption("Requisito: Consulta com operador de conjunto (EXCEPT). Identifica palestras que possuem inscrições registradas, mas que ainda não receberam nenhum feedback.")
    colunas, dados = consulta_conjunto_2_palestras_sem_feedback(snowpark_session)
    if dados and colunas:
        df = pd.DataFrame(dados, columns=[c.upper() for c in colunas])
        st.dataframe(df, use_container_width=True)
    else:
        st.info("Todas as palestras com inscrições têm feedback, ou não há dados.")


# --- NAVEGAÇÃO PRINCIPAL (ROUTER) ---

if 'page' not in st.session_state:
    st.session_state.page = 'Home'

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
    "Feedback": "💬 Coleta de Feedback",
    "Consultas": "📊 Relatórios e Análise (Fase 4)"
}

st.sidebar.subheader("Navegação Principal")
for key, value in MODULOS.items():
    if st.sidebar.button(value, key=f"nav_btn_{key}"):
        st.session_state.page = key

st.sidebar.markdown("---")
st.sidebar.caption("Exercício-Programa 2/25 - USP EACH")

def router():
    page = st.session_state.page

    if page == 'Home':
        st.title("Bem-vindo à Plataforma de Gerenciamento de Eventos")
        st.info("Utilize o menu lateral para acessar as funcionalidades de Manutenção e Relatórios.")
        st.markdown(f"**Status da Entrega (Fase 4):** 7/7 tabelas e 6/6 consultas complexas implementadas. Acesso a dados via Snowpark.")

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

if snowpark_session:
    router()
else:
    st.error("Não foi possível estabelecer a conexão com o Snowflake/Snowpark. Verifique as credenciais no arquivo `snowpark_utils.py`.")