import os
import json
from datetime import datetime, date
from io import BytesIO

import pandas as pd
import streamlit as st
from supabase import create_client, Client

# -------------------- CONFIG --------------------
st.set_page_config(page_title="Acompanhamento de Obras", page_icon="🏗️", layout="wide")

SUPABASE_URL = st.secrets.get("SUPABASE_URL", os.getenv("SUPABASE_URL"))
SUPABASE_ANON_KEY = st.secrets.get("SUPABASE_ANON_KEY", os.getenv("SUPABASE_ANON_KEY"))
SUPABASE_BUCKET = st.secrets.get("SUPABASE_BUCKET", os.getenv("SUPABASE_BUCKET", "obra-uploads"))

if not SUPABASE_URL or not SUPABASE_ANON_KEY:
    st.error("Config do Supabase ausente. Preencha SUPABASE_URL e SUPABASE_ANON_KEY nos Secrets.")
    st.stop()

sb: Client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)

# -------------------- Helpers / DB --------------------
def sb_select(table, select="*", filters=None, order=None, limit=None):
    q = sb.table(table).select(select)
    if filters:
        for k, v in filters.items():
            if isinstance(v, tuple) and len(v) == 2 and v[0] == "in":
                q = q.in_(k, v[1])
            else:
                q = q.eq(k, v)
    if order:
        if isinstance(order, (list, tuple)):
            for o in order:
                q = q.order(o)
        else:
            q = q.order(order)
    if limit:
        q = q.limit(limit)
    res = q.execute()
    return res.data or []

def sb_insert(table, data):
    res = sb.table(table).insert(data).execute()
    return res.data or []

def sb_upsert(table, data, on_conflict=None):
    q = sb.table(table).upsert(data)
    if on_conflict:
        q = q.on_conflict(on_conflict)
    res = q.execute()
    return res.data or []

def sb_update(table, data, filters):
    q = sb.table(table).update(data)
    for k, v in filters.items():
        q = q.eq(k, v)
    res = q.execute()
    return res.data or []

def sb_delete(table, filters):
    q = sb.table(table).delete()
    for k, v in filters.items():
        q = q.eq(k, v)
    res = q.execute()
    return res.data or []

def log_event(usuario, acao, obra_id=None, casa_id=None, servico_id=None, detalhes=None):
    try:
        sb_insert("auditoria", {
            "timestamp": datetime.utcnow().isoformat(),
            "usuario": usuario,
            "acao": acao,
            "obra_id": obra_id,
            "casa_id": casa_id,
            "servico_id": servico_id,
            "detalhes": detalhes if isinstance(detalhes, dict) else json.dumps(detalhes) if detalhes else None
        })
    except Exception:
        pass

# -------------------- Auth --------------------
def _default_permissoes(role="user"):
    base = {
        "ver_ativar_casa": True,
        "ver_lancamentos": True,
        "ver_dashboard": True,
        "ver_servicos": False,
        "ver_logs": False,
        "ver_admin": False,
        "editar_lancamentos": True,
        "editar_servicos": False,
        "editar_usuarios": False,
        "corrigir_registros": False,
    }
    if role == "admin":
        for k in base:
            base[k] = True
    return base

def _merge_permissoes(user_dict):
    role = user_dict.get("role", "user")
    p = dict(_default_permissoes(role))
    up_raw = user_dict.get("permissoes")
    up = up_raw if isinstance(up_raw, dict) else (json.loads(up_raw) if isinstance(up_raw, str) and up_raw.strip().startswith("{") else {})
    for k in p:
        p[k] = bool(up.get(k, p[k]))
    return p

def ensure_admin_seed():
    users = sb_select("usuarios", limit=1)
    if not users:
        sb_insert("usuarios", {
            "username": "admin",
            "nome": "Administrador",
            "password": "123456",
            "role": "admin",
            "ativo": True,
            "permissoes": {}
        })

ensure_admin_seed()

def check_login(username, password):
    rows = sb_select("usuarios", filters={"username": username, "password": password, "ativo": True}, limit=1)
    if rows:
        u = rows[0]
        return {"username": u["username"], "nome": u.get("nome", u["username"]), "role": u.get("role", "user"), "permissoes": u.get("permissoes", {})}
    return None

def can_view(page_name):
    user = st.session_state.get("user", {})
    p = _merge_permissoes(user)
    mapping = {
        "Ativar Casa": "ver_ativar_casa",
        "Lançamentos": "ver_lancamentos",
        "Dashboard": "ver_dashboard",
        "Observações": True,
        "Base de Dados": "ver_servicos",
        "Logs": "ver_logs",
        "Correções": "corrigir_registros",
        "Admin": "ver_admin",
        "Minha Conta": True,
    }
    key = mapping.get(page_name, True)
    return p.get(key, True) if isinstance(key, str) else bool(key)

def can_edit(action_key):
    user = st.session_state.get("user", {})
    p = _merge_permissoes(user)
    return bool(p.get(action_key, False))

# -------------------- Storage: upload foto --------------------
def upload_foto(file_bytes, filename):
    try:
        path = f"{filename}"
        res = sb.storage.from_(SUPABASE_BUCKET).upload(path, file_bytes)
        if isinstance(res, dict) and res.get("error"):
            st.error(f"Erro ao enviar imagem: {res['error']['message']}")
            return None
        return sb.storage.from_(SUPABASE_BUCKET).get_public_url(path)
    except Exception as e:
        st.error(f"Falha no upload: {e}")
        return None

# -------------------- Sidebar / Login --------------------
st.sidebar.title("🏗️ Acompanhamento de Obras — Login")

if "user" not in st.session_state or not st.session_state["user"]:
    with st.form("login"):
        st.subheader("Entrar")
        u = st.text_input("Usuário")
        p = st.text_input("Senha", type="password")
        ok = st.form_submit_button("Entrar")
        if ok:
            user = check_login(u, p)
            if user:
                st.session_state["user"] = user
                st.success(f"Bem-vindo, {user['nome']}!")
                st.rerun()
            else:
                st.error("Usuário ou senha inválidos, ou usuário inativo.")
    st.stop()

user = st.session_state["user"]
st.sidebar.write(f"**Usuário:** {user['nome']}  \n**Perfil:** {user['role']}")
if st.sidebar.button("Sair"):
    st.session_state.pop("user", None)
    st.rerun()

pages_all = ["Ativar Casa", "Lançamentos", "Dashboard", "Observações", "Base de Dados", "Logs", "Correções", "Admin", "Minha Conta"]
pages = [p for p in pages_all if can_view(p)]
page = st.sidebar.radio("Navegação", pages)

# -------------------- Minha Conta --------------------
if page == "Minha Conta":
    st.header("Minha Conta")
    st.write(f"Usuário: **{user.get('username','')}**")
    st.write(f"Nome: **{user['nome']}**")

    st.subheader("Alterar senha")
    with st.form("form_change_password"):
        col1, col2, col3 = st.columns(3)
        current_pw = col1.text_input("Senha atual", type="password")
        new_pw = col2.text_input("Nova senha", type="password")
        confirm_pw = col3.text_input("Confirmar nova senha", type="password")
        submitted = st.form_submit_button("Salvar nova senha")
        if submitted:
            if not current_pw or not new_pw or not confirm_pw:
                st.error("Preencha todos os campos.")
            elif new_pw != confirm_pw:
                st.error("A confirmação não confere com a nova senha.")
            elif len(new_pw) < 4:
                st.error("A nova senha deve ter pelo menos 4 caracteres.")
            else:
                rows = sb_select("usuarios", filters={"username": user["username"], "password": current_pw, "ativo": True}, limit=1)
                if not rows:
                    st.error("Senha atual incorreta.")
                else:
                    sb_update("usuarios", {"password": new_pw}, {"username": user["username"]})
                    st.success("Senha alterada com sucesso.")
                    log_event(user["nome"], "alterar_senha", detalhes={"username": user["username"]})

# -------------------- Base de Dados (CRUD + Importações) --------------------
if page == "Base de Dados" and can_view("Base de Dados"):
    st.header("Base de Dados")
    tabs = st.tabs(["Obras", "Etapas", "Serviços por Etapa", "Casas"])

    # --- Obras ---
    with tabs[0]:
        st.subheader("Obras")
        with st.form("nova_obra"):
            nome_obra = st.text_input("Nome da Obra", placeholder="Ex.: Berlin")
            ok = st.form_submit_button("Criar Obra")
            if ok and nome_obra.strip():
                try:
                    sb_insert("obras", {"nome": nome_obra.strip()})
                    st.success(f"Obra '{nome_obra}' criada.")
                    log_event(user["nome"], "criar_obra", detalhes={"obra": nome_obra.strip()})
                except Exception as e:
                    st.error(f"Não foi possível criar a obra: {e}")
        obras = pd.DataFrame(sb_select("obras", order="nome"))
        st.dataframe(obras[["id","nome"]] if not obras.empty else obras, use_container_width=True, hide_index=True)

        st.markdown("#### Excluir obra")
        if not obras.empty:
            ob_del_nome = st.selectbox("Selecione a obra para excluir", obras["nome"].tolist(), key="obra_del_nome")
            ob_id = int(obras.loc[obras["nome"]==ob_del_nome, "id"].iloc[0])
            col_a, col_b = st.columns([1,2])
            st.caption("A exclusão remove casas, etapas, serviços, lançamentos e estados vinculados (via ON DELETE CASCADE).")
            conf_txt = col_b.text_input('Digite "EXCLUIR" para confirmar', key="obra_del_conf")
            btn_del = st.button("🗑️ Excluir obra selecionada", type="primary", disabled=(conf_txt.strip().upper()!="EXCLUIR"))
            if btn_del:
                try:
                    sb_delete("obras", {"id": ob_id})
                    st.success(f"Obra '{ob_del_nome}' excluída.")
                    log_event(user["nome"], "excluir_obra", obra_id=ob_id, detalhes={"obra": ob_del_nome})
                    st.rerun()
                except Exception as e:
                    st.error(f"Falha ao excluir obra: {e}")

    # --- Etapas ---
    with tabs[1]:
        st.subheader("Etapas por Obra")
        obras = pd.DataFrame(sb_select("obras", order="nome"))
        if obras.empty:
            st.info("Crie uma obra primeiro.")
        else:
            obra_sel = st.selectbox("Obra", obras["nome"].tolist(), key="bd_et_ob")
            obra_id = int(obras.loc[obras["nome"] == obra_sel, "id"].iloc[0])
            with st.form("nova_etapa"):
                etapa_nome = st.text_input("Nova Etapa", placeholder="Ex.: Reboco")
                ok_e = st.form_submit_button("Adicionar Etapa")
                if ok_e and etapa_nome.strip():
                    try:
                        sb_insert("etapas", {"obra_id": obra_id, "nome": etapa_nome.strip()})
                        st.success(f"Etapa '{etapa_nome}' criada para {obra_sel}.")
                        log_event(user["nome"], "criar_etapa", obra_id=obra_id, detalhes={"etapa": etapa_nome.strip()})
                    except Exception as e:
                        st.error(f"Não foi possível criar a etapa: {e}")
            etapas = pd.DataFrame(sb_select("etapas", filters={"obra_id": obra_id}, order="nome"))
            st.dataframe(etapas[["id","nome"]] if not etapas.empty else etapas, use_container_width=True, hide_index=True)

            st.markdown("#### Excluir etapa")
            if not etapas.empty:
                et_del_nome = st.selectbox("Etapa para excluir", etapas["nome"].tolist(), key="et_del_nome")
                btn_del = st.button("🗑️ Excluir etapa", type="primary")
                if btn_del:
                    try:
                        # Remove serviços da etapa (e dependências via FK)
                        servs = sb_select("servicos", filters={"obra_id": obra_id, "etapa": et_del_nome})
                        for s in servs:
                            sb_delete("servicos", {"id": s["id"]})
                        # Remove ativações dessa etapa
                        casas = sb_select("casas", filters={"obra_id": obra_id})
                        casa_ids = [c["id"] for c in casas]
                        if casa_ids:
                            for cid in casa_ids:
                                sb_delete("casa_ativacoes", {"casa_id": cid, "etapa": et_del_nome})
                        # Remove a etapa
                        sb_delete("etapas", {"obra_id": obra_id, "nome": et_del_nome})
                        st.success(f"Etapa '{et_del_nome}' excluída.")
                        log_event(user["nome"], "excluir_etapa", obra_id=obra_id, detalhes={"etapa": et_del_nome})
                        st.rerun()
                    except Exception as e:
                        st.error(f"Falha ao excluir etapa: {e}")

    # --- Serviços por Etapa (manual + importação) ---
    with tabs[2]:
        st.subheader("Serviços por Etapa")
        obras = pd.DataFrame(sb_select("obras", order="nome"))
        if obras.empty:
            st.info("Crie uma obra primeiro.")
        else:
            obra_sel = st.selectbox("Obra", obras["nome"].tolist(), key="bd_sv_ob")
            obra_id = int(obras.loc[obras["nome"] == obra_sel, "id"].iloc[0])
            etapas = pd.DataFrame(sb_select("etapas", filters={"obra_id": obra_id}, order="nome"))
            if etapas.empty:
                st.info("Cadastre uma etapa para esta obra.")
            else:
                etapa_sel = st.selectbox("Etapa (para cadastro manual e importação sem coluna 'etapa')", etapas["nome"].tolist(), key="bd_sv_et")

                # Cadastro manual
                with st.form("novo_servico"):
                    nome_serv = st.text_input("Nome do Serviço", placeholder="Ex.: Chap. Ext. + Tela")
                    ok_s = st.form_submit_button("Adicionar Serviço")
                    if ok_s and nome_serv.strip():
                        try:
                            sb_insert("servicos", {"nome": nome_serv.strip(), "etapa": etapa_sel, "obra_id": obra_id})
                            st.success(f"Serviço '{nome_serv}' adicionado em {etapa_sel} ({obra_sel}).")
                            log_event(user["nome"], "criar_servico", obra_id=obra_id, detalhes={"etapa": etapa_sel, "servico": nome_serv.strip()})
                        except Exception as e:
                            st.error(f"Não foi possível adicionar o serviço: {e}")

                # Lista
                servs = pd.DataFrame(sb_select("servicos", filters={"obra_id": obra_id, "etapa": etapa_sel}, order="nome"))
                st.dataframe(servs[["id","nome"]] if not servs.empty else servs, use_container_width=True, hide_index=True)

                # Importação em massa
                st.markdown("### Importar serviços por planilha (.xlsx ou .csv)")
                with st.expander("Modelo de planilha e instruções", expanded=False):
                    st.write("- **Opção A (1 coluna):** `servico` — todos serão criados na **etapa selecionada** acima.")
                    st.write("- **Opção B (2 colunas):** `etapa`, `servico` — cada linha usa sua própria etapa.")
                    st.write("Observação: duplicados são ignorados/atualizados automaticamente (upsert).")

                fserv = st.file_uploader("Escolher arquivo de serviços", type=["xlsx","csv"], key="file_serv")
                if fserv is not None:
                    try:
                        if fserv.name.lower().endswith(".csv"):
                            df_import = pd.read_csv(fserv)
                        else:
                            df_import = pd.read_excel(fserv)
                        # normalizar colunas
                        cols = [c.strip().lower() for c in df_import.columns]
                        df_import.columns = cols

                        registros = []
                        if "servico" in df_import.columns and "etapa" in df_import.columns:
                            for _, r in df_import.iterrows():
                                nome = str(r.get("servico", "")).strip()
                                etp = str(r.get("etapa", "")).strip() or etapa_sel
                                if nome:
                                    registros.append({"nome": nome, "etapa": etp, "obra_id": obra_id})
                        elif "servico" in df_import.columns:
                            for _, r in df_import.iterrows():
                                nome = str(r.get("servico", "")).strip()
                                if nome:
                                    registros.append({"nome": nome, "etapa": etapa_sel, "obra_id": obra_id})
                        else:
                            st.error("A planilha precisa ter a coluna 'servico' (e opcionalmente 'etapa').")
                            registros = []

                        if registros:
                            # upsert em lote
                            # on_conflict -> (nome, etapa, obra_id)
                            # dividimos em pedaços p/ evitar payloads muito grandes
                            chunk = 500
                            total_ok = 0
                            for i in range(0, len(registros), chunk):
                                sb_upsert("servicos", registros[i:i+chunk], on_conflict="nome,etapa,obra_id")
                                total_ok += len(registros[i:i+chunk])
                            st.success(f"Importação concluída. Serviços processados: {total_ok}.")
                            log_event(user["nome"], "importar_servicos", obra_id=obra_id, detalhes={"total": total_ok})
                            st.rerun()
                    except Exception as e:
                        st.error(f"Falha ao importar serviços: {e}")

                # Excluir serviço
                st.markdown("#### Excluir serviço")
                if not servs.empty:
                    srv_del_nome = st.selectbox("Serviço para excluir", servs["nome"].tolist(), key="srv_del_nome")
                    if st.button("🗑️ Excluir serviço", type="primary"):
                        try:
                            srv_id = int(servs.loc[servs["nome"]==srv_del_nome, "id"].iloc[0])
                            sb_delete("servicos", {"id": srv_id})
                            st.success(f"Serviço '{srv_del_nome}' excluído.")
                            log_event(user["nome"], "excluir_servico", obra_id=obra_id, servico_id=srv_id, detalhes={"etapa": etapa_sel, "servico": srv_del_nome})
                            st.rerun()
                        except Exception as e:
                            st.error(f"Falha ao excluir serviço: {e}")

    # --- Casas (manual + importação) ---
    with tabs[3]:
        st.subheader("Casas")
        obras = pd.DataFrame(sb_select("obras", order="nome"))
        if obras.empty:
            st.info("Crie uma obra primeiro.")
        else:
            obra_sel = st.selectbox("Obra", obras["nome"].tolist(), key="bd_casa_ob")
            obra_id = int(obras.loc[obras["nome"] == obra_sel, "id"].iloc[0])

            # Cadastro manual
            with st.form("nova_casa"):
                lote = st.text_input("Lote (Identificador)", placeholder="Ex.: QD 3 LT 15")
                cod_tip = st.text_input("Código Tipologia", value="")
                tip = st.text_input("Tipologia", value="")
                okc = st.form_submit_button("Adicionar Casa")
                if okc and lote.strip():
                    try:
                        sb_insert("casas", {"obra_id": obra_id, "lote": lote.strip(), "cod_tipologia": cod_tip, "tipologia": tip})
                        st.success(f"Casa '{lote}' criada.")
                        log_event(user["nome"], "criar_casa", obra_id=obra_id, detalhes={"lote": lote.strip()})
                    except Exception as e:
                        st.error(f"Não foi possível criar a casa: {e}")

            casas = pd.DataFrame(sb_select("casas", filters={"obra_id": obra_id}, order="lote"))
            st.dataframe(casas[["id","lote","tipologia","ativa","ativa_em","ativa_por"]] if not casas.empty else casas, use_container_width=True, hide_index=True)

            # Importação em massa de casas
            st.markdown("### Importar casas por planilha (.xlsx ou .csv)")
            with st.expander("Modelo de planilha e instruções", expanded=False):
                st.write("- **Opção A (1 coluna):** `lote` (ex.: `QD 3 LT 15`).")
                st.write("- **Opção B (2 colunas):** `quadra`, `lote` → o sistema gera `QD {quadra} LT {lote}`.")
                st.write("Colunas extras (ex.: `cod_tipologia`, `tipologia`) são opcionais e, se existirem, serão importadas.")
                st.write("Duplicados são ignorados/atualizados automaticamente (upsert).")

            fcasas = st.file_uploader("Escolher arquivo de casas", type=["xlsx","csv"], key="file_casas")
            if fcasas is not None:
                try:
                    if fcasas.name.lower().endswith(".csv"):
                        dfc = pd.read_csv(fcasas)
                    else:
                        dfc = pd.read_excel(fcasas)
                    cols = [c.strip().lower() for c in dfc.columns]
                    dfc.columns = cols

                    registros = []
                    # Preferência: se vier 'lote' usa diretamente.
                    if "lote" in dfc.columns:
                        for _, r in dfc.iterrows():
                            lote_val = str(r.get("lote", "")).strip()
                            if not lote_val:
                                continue
                            rec = {
                                "obra_id": obra_id,
                                "lote": lote_val,
                                "cod_tipologia": str(r.get("cod_tipologia", "") or "").strip() if "cod_tipologia" in dfc.columns else None,
                                "tipologia": str(r.get("tipologia", "") or "").strip() if "tipologia" in dfc.columns else None,
                            }
                            registros.append(rec)
                    # Caso não tenha 'lote', mas tenha 'quadra' e 'lote' (numérico/str separado)
                    elif ("quadra" in dfc.columns) and ("lote" in dfc.columns):
                        # (este ramo só cai se 'lote' não foi detectado na normalização — mantido por compatibilidade)
                        pass
                    # Caso 2 colunas: quadra + lote_num (permitindo nomes alternativos)
                    else:
                        # tentar variações
                        q_col = None
                        l_col = None
                        for c in dfc.columns:
                            if c in ["quadra", "qd", "q"]:
                                q_col = c
                            if c in ["lote", "lt", "l"]:
                                l_col = c
                        if q_col and l_col:
                            for _, r in dfc.iterrows():
                                qv = str(r.get(q_col, "")).strip()
                                lv = str(r.get(l_col, "")).strip()
                                if not qv or not lv:
                                    continue
                                lote_val = f"QD {qv} LT {lv}"
                                rec = {
                                    "obra_id": obra_id,
                                    "lote": lote_val,
                                    "cod_tipologia": str(r.get("cod_tipologia", "") or "").strip() if "cod_tipologia" in dfc.columns else None,
                                    "tipologia": str(r.get("tipologia", "") or "").strip() if "tipologia" in dfc.columns else None,
                                }
                                registros.append(rec)

                    if not registros:
                        st.error("Não foram encontrados dados válidos. Certifique-se de ter 'lote' ou 'quadra'+'lote' na planilha.")
                    else:
                        # upsert em lote por (obra_id, lote)
                        chunk = 500
                        total_ok = 0
                        for i in range(0, len(registros), chunk):
                            sb_upsert("casas", registros[i:i+chunk], on_conflict="obra_id,lote")
                            total_ok += len(registros[i:i+chunk])
                        st.success(f"Importação concluída. Casas processadas: {total_ok}.")
                        log_event(user["nome"], "importar_casas", obra_id=obra_id, detalhes={"total": total_ok})
                        st.rerun()
                except Exception as e:
                    st.error(f"Falha ao importar casas: {e}")

            # Excluir casa
            st.markdown("#### Excluir casa")
            if not casas.empty:
                casa_del_lote = st.selectbox("Casa (lote) para excluir", casas["lote"].tolist(), key="casa_del")
                if st.button("🗑️ Excluir casa", type="primary"):
                    try:
                        cid = int(casas.loc[casas["lote"]==casa_del_lote, "id"].iloc[0])
                        sb_delete("casas", {"id": cid})
                        st.success(f"Casa '{casa_del_lote}' excluída.")
                        log_event(user["nome"], "excluir_casa", obra_id=obra_id, casa_id=cid, detalhes={"lote": casa_del_lote})
                        st.rerun()
                    except Exception as e:
                        st.error(f"Falha ao excluir casa: {e}")

# -------------------- Logs --------------------
if page == "Logs" and can_view("Logs"):
    st.header("Logs do Sistema")
    st.caption("Registro de tudo que foi feito: quem, quando e o que.")

    usuarios = pd.DataFrame(sb_select("auditoria", select="usuario", order="usuario"))
    usuarios_list = sorted(set([u["usuario"] for u in usuarios])) if not usuarios.empty else []
    col1, col2, col3 = st.columns(3)
    usuario_sel = col1.selectbox("Usuário", ["Todos"] + usuarios_list)
    acoes = pd.DataFrame(sb_select("auditoria", select="acao", order="acao"))
    acoes_list = sorted(set([a["acao"] for a in acoes])) if not acoes.empty else []
    acao_sel = col2.selectbox("Ação", ["Todas"] + acoes_list)
    limite = col3.number_input("Mostrar últimos (registros)", min_value=50, max_value=100000, value=200, step=50)

    rows = sb_select("auditoria", order="id")
    df_logs = pd.DataFrame(rows)
    if not df_logs.empty:
        if usuario_sel != "Todos":
            df_logs = df_logs[df_logs["usuario"] == usuario_sel]
        if acao_sel != "Todas":
            df_logs = df_logs[df_logs["acao"] == acao_sel]
        df_logs = df_logs.sort_values("id", ascending=False).head(int(limite))
    st.dataframe(df_logs if not df_logs.empty else pd.DataFrame(), use_container_width=True)

    if not df_logs.empty and st.button("Exportar CSV"):
        csv_bytes = df_logs.to_csv(index=False).encode("utf-8-sig")
        st.download_button("Baixar CSV", data=csv_bytes, file_name="logs.csv", mime="text/csv")

# -------------------- Admin (Usuários) --------------------
if page == "Admin" and can_view("Admin") and can_edit("editar_usuarios"):
    st.header("Administração de Usuários")

    users = pd.DataFrame(sb_select("usuarios", order="username"))
    if users.empty:
        st.info("Nenhum usuário na base.")
    else:
        def _perm_resumo(u):
            p = _merge_permissoes(u)
            chaves = ["ver_ativar_casa","ver_lancamentos","ver_dashboard","ver_servicos","ver_logs","ver_admin",
                      "editar_lancamentos","editar_servicos","editar_usuarios","corrigir_registros"]
            return ", ".join([k for k in chaves if p.get(k)])
        users["permissoes_resumo"] = users.apply(_perm_resumo, axis=1)
        cols_show = [c for c in ["username","nome","role","ativo","permissoes_resumo"] if c in users.columns]
        st.dataframe(users[cols_show], use_container_width=True)

    st.divider()
    st.subheader("Criar usuário")
    with st.form("new_user"):
        nu_user = st.text_input("Usuário (login)")
        nu_nome = st.text_input("Nome completo")
        nu_role = st.selectbox("Perfil", ["user", "admin"])
        nu_pw = st.text_input("Senha inicial", type="password", value="123456")
        base_p = _default_permissoes(nu_role)
        c1, c2, c3 = st.columns(3)
        with c1:
            v_ac = st.checkbox("Ver Ativar Casa", value=base_p["ver_ativar_casa"])
            v_la = st.checkbox("Ver Lançamentos", value=base_p["ver_lancamentos"])
            v_da = st.checkbox("Ver Dashboard", value=base_p["ver_dashboard"])
        with c2:
            v_se = st.checkbox("Ver Serviços (Base de Dados)", value=base_p["ver_servicos"])
            v_lo = st.checkbox("Ver Logs", value=base_p["ver_logs"])
            v_ad = st.checkbox("Ver Admin", value=base_p["ver_admin"])
        with c3:
            e_la = st.checkbox("Editar Lançamentos", value=base_p["editar_lancamentos"])
            e_se = st.checkbox("Editar Serviços", value=base_p["editar_servicos"])
            e_us = st.checkbox("Editar Usuários", value=base_p["editar_usuarios"])
            c_rr = st.checkbox("Corrigir Registros (Correções)", value=base_p.get("corrigir_registros", False))

        ok_new = st.form_submit_button("Criar")
        if ok_new and nu_user.strip():
            if any(u["username"] == nu_user for u in users.to_dict(orient="records")) if not users.empty else False:
                st.error("Usuário já existe.")
            else:
                perms = {
                    "ver_ativar_casa": v_ac, "ver_lancamentos": v_la, "ver_dashboard": v_da,
                    "ver_servicos": v_se, "ver_logs": v_lo, "ver_admin": v_ad,
                    "editar_lancamentos": e_la, "editar_servicos": e_se, "editar_usuarios": e_us, "corrigir_registros": c_rr,
                }
                sb_insert("usuarios", {
                    "username": nu_user.strip(), "nome": nu_nome.strip(), "role": nu_role,
                    "password": nu_pw, "ativo": True, "permissoes": perms
                })
                st.success("Usuário criado.")
                log_event(user["nome"], "criar_usuario", detalhes={"username": nu_user, "nome": nu_nome, "role": nu_role})
                st.rerun()

    st.divider()
    st.subheader("Editar usuário existente")
    users = pd.DataFrame(sb_select("usuarios", order="username"))
    if not users.empty:
        opts = {f"{r['nome']} ({r['username']})": r["username"] for _, r in users.iterrows()}
        sel = st.selectbox("Selecione um usuário", list(opts.keys()))
        uname = opts[sel]
        u = users[users["username"] == uname].iloc[0].to_dict()
        with st.form("edit_user"):
            eu_nome = st.text_input("Nome completo", value=u.get("nome",""))
            eu_role = st.selectbox("Perfil", ["user","admin"], index=0 if u.get("role")=="user" else 1)
            eu_status = st.selectbox("Status", ["Ativo","Inativo"], index=0 if u.get("ativo", True) else 1)
            eu_pw_reset = st.checkbox("Resetar senha para 123456")
            base_p = _merge_permissoes({"role": eu_role, "permissoes": u.get("permissoes")})
            c1, c2, c3 = st.columns(3)
            with c1:
                v_ac = st.checkbox("Ver Ativar Casa", value=base_p["ver_ativar_casa"])
                v_la = st.checkbox("Ver Lançamentos", value=base_p["ver_lancamentos"])
                v_da = st.checkbox("Ver Dashboard", value=base_p["ver_dashboard"])
            with c2:
                v_se = st.checkbox("Ver Serviços (Base de Dados)", value=base_p["ver_servicos"])
                v_lo = st.checkbox("Ver Logs", value=base_p["ver_logs"])
                v_ad = st.checkbox("Ver Admin", value=base_p["ver_admin"])
            with c3:
                e_la = st.checkbox("Editar Lançamentos", value=base_p["editar_lancamentos"])
                e_se = st.checkbox("Editar Serviços", value=base_p["editar_servicos"])
                e_us = st.checkbox("Editar Usuários", value=base_p["editar_usuarios"])
                c_rr = st.checkbox("Corrigir Registros (Correções)", value=base_p.get("corrigir_registros", False))

            ok_m = st.form_submit_button("Salvar alterações")
            if ok_m:
                update = {
                    "nome": eu_nome.strip(),
                    "role": eu_role,
                    "ativo": (eu_status == "Ativo"),
                    "permissoes": {
                        "ver_ativar_casa": v_ac, "ver_lancamentos": v_la, "ver_dashboard": v_da,
                        "ver_servicos": v_se, "ver_logs": v_lo, "ver_admin": v_ad,
                        "editar_lancamentos": e_la, "editar_servicos": e_se, "editar_usuarios": e_us, "corrigir_registros": c_rr,
                    }
                }
                if eu_pw_reset:
                    update["password"] = "123456"
                sb_update("usuarios", update, {"username": uname})
                st.success("Alterações salvas.")
                log_event(user["nome"], "alterar_usuario", detalhes={"username": uname, "role": eu_role, "ativo": update["ativo"]})

# -------------------- Correções (simplificado) --------------------
if page == "Correções" and can_view("Correções") and can_edit("corrigir_registros"):
    st.header("Correções")
    st.caption("Anule lançamentos e ajuste estado de serviço; tudo vai para auditoria.")

    obras = pd.DataFrame(sb_select("obras", order="nome"))
    if obras.empty:
        st.info("Não há obras cadastradas.")
    else:
        obra_nome = st.selectbox("Obra", obras["nome"].tolist(), key="cor_ob")
        obra_id = int(obras.loc[obras["nome"]==obra_nome, "id"].iloc[0])

        casas = pd.DataFrame(sb_select("casas", filters={"obra_id": obra_id}, order="lote"))
        lanc = pd.DataFrame(sb_select("lancamentos", filters={"obra_id": obra_id}))
        if casas.empty or lanc.empty:
            st.info("Não há casas/lançamentos nesta obra.")
        else:
            st.subheader("Casas com lançamentos")
            casas_lanc = lanc[~lanc["anulado"]].merge(casas[["id","lote"]], left_on="casa_id", right_on="id", how="inner")
            if casas_lanc.empty:
                st.info("Não há lançamentos ativos.")
            else:
                lotes = sorted(casas_lanc["lote"].unique().tolist())
                lote_sel = st.selectbox("Casa (lote)", lotes)
                casa_id = int(casas[cases := (casas["lote"]==lote_sel)]["id"].iloc[0])

                servs = pd.DataFrame(sb_select("servicos", filters={"obra_id": obra_id}))
                if servs.empty:
                    st.info("Sem serviços.")
                else:
                    lan_casa = lanc[(lanc["casa_id"]==casa_id) & (~lanc["anulado"])].copy()
                    lan_casa = lan_casa.merge(servs[["id","nome","etapa"]], left_on="servico_id", right_on="id", how="left", suffixes=("","_srv"))
                    etapa_opts = ["Todas"] + sorted([e for e in lan_casa["etapa"].dropna().unique().tolist()])
                    etapa_sel = st.selectbox("Etapa", etapa_opts)
                    if etapa_sel != "Todas":
                        lan_casa = lan_casa[lan_casa["etapa"] == etapa_sel]
                    serv_opts = sorted(lan_casa["nome"].dropna().unique().tolist())
                    if not serv_opts:
                        st.info("Sem lançamentos nesta seleção.")
                    else:
                        serv_nome = st.selectbox("Serviço", serv_opts)
                        sid = int(servs.loc[servs["nome"]==serv_nome, "id"].iloc[0])

                        st.subheader("Últimos lançamentos (ativos)")
                        ults = lan_casa[lan_casa["servico_id"]==sid].sort_values("created_at", ascending=False).head(5)
                        st.dataframe(ults[["status","responsavel","executor","data_inicio","data_conclusao","observacoes","created_at"]], use_container_width=True)

                        st.divider()
                        ca, cb = st.columns(2)
                        with ca:
                            st.markdown("### Anular último lançamento")
                            motivo = st.text_input("Motivo (obrigatório)", key="cor_mot_anul")
                            if st.button("Anular último"):
                                if not motivo.strip():
                                    st.error("Informe o motivo.")
                                else:
                                    ult = ults.head(1)
                                    if ult.empty:
                                        st.info("Não há lançamentos para anular.")
                                    else:
                                        lid = int(ult.iloc[0]["id"])
                                        sb_update("lancamentos", {"anulado": True, "anulado_por": user["nome"], "anulado_em": datetime.utcnow().isoformat(), "anulacao_motivo": motivo}, {"id": lid})
                                        st.success("Lançamento anulado.")
                                        log_event(user["nome"], "anular_lancamento", obra_id=obra_id, casa_id=casa_id, servico_id=sid, detalhes={"motivo": motivo, "lancamento_id": lid})
                                        st.rerun()
                        with cb:
                            st.markdown("### Ajustar estado do serviço")
                            estado = pd.DataFrame(sb_select("estado_servicos", filters={"casa_id": casa_id, "servico_id": sid}, limit=1))
                            cur_status = estado["status"].iloc[0] if not estado.empty else "Não iniciado"
                            cur_exec = estado["executor"].iloc[0] if not estado.empty else ""
                            cur_ini = estado["data_inicio"].iloc[0] if not estado.empty else None
                            cur_fim = estado["data_fim"].iloc[0] if not estado.empty else None
                            novo_status = st.selectbox("Novo status", ["Não iniciado","Em execução","Concluído"], index=["Não iniciado","Em execução","Concluído"].index(cur_status) if cur_status in ["Não iniciado","Em execução","Concluído"] else 0)
                            novo_executor = st.text_input("Executor", value=cur_exec or "")
                            cold1, cold2 = st.columns(2)
                            nova_ini = cold1.date_input("Data de início", value=(date.fromisoformat(cur_ini) if cur_ini else date.today()))
                            nova_fim = cold2.date_input("Data de conclusão", value=(date.fromisoformat(cur_fim) if cur_fim else date.today()))
                            motivo_estado = st.text_input("Motivo (obrigatório)")
                            if st.button("Salvar estado"):
                                if not motivo_estado.strip():
                                    st.error("Informe o motivo.")
                                else:
                                    now = datetime.utcnow().isoformat()
                                    rec = {"casa_id": casa_id, "servico_id": sid, "status": novo_status, "executor": novo_executor or "", "data_inicio": nova_ini.isoformat(), "data_fim": (nova_fim.isoformat() if novo_status=="Concluído" else None), "updated_at": now}
                                    sb_upsert("estado_servicos", rec, on_conflict="casa_id,servico_id")
                                    st.success("Estado atualizado.")
                                    log_event(user["nome"], "editar_estado_servico", obra_id=obra_id, casa_id=casa_id, servico_id=sid, detalhes={"motivo": motivo_estado, "novo_status": novo_status, "novo_executor": novo_executor})
                                    st.rerun()

# -------------------- Ativar Casa --------------------
if page == "Ativar Casa":
    st.header("Ativar Casa (por frente de serviço)")
    obras = pd.DataFrame(sb_select("obras", order="nome"))
    if obras.empty:
        st.warning("Não há obras cadastradas.")
    else:
        obra_nome = st.selectbox("Obra", obras["nome"].tolist())
        obra_id = int(obras.loc[obras["nome"]==obra_nome, "id"].iloc[0])
        casas = pd.DataFrame(sb_select("casas", filters={"obra_id": obra_id}, order="lote"))
        if casas.empty:
            st.info("Cadastre casas na aba Base de Dados → Casas.")
            st.stop()
        lote = st.selectbox("Lote (Identificador)", casas["lote"].tolist())
        casa_id = int(casas.loc[casas["lote"]==lote, "id"].iloc[0])
        etapa = st.selectbox("Frente de serviço (etapa)", ["Reboco","Pintura","Revestimento"], index=0)

        ativ = pd.DataFrame(sb_select("casa_ativacoes", filters={"casa_id": casa_id, "etapa": etapa}, limit=1))
        ativa_flag = bool(ativ["ativa"].iloc[0]) if not ativ.empty else False
        ativa_em = ativ["ativa_em"].iloc[0] if not ativ.empty else None
        ativa_por = ativ["ativa_por"].iloc[0] if not ativ.empty else None

        if ativa_flag:
            st.success(f"Casa {lote} — {etapa} já está ATIVA desde {ativa_em} por {ativa_por or '—'}.")
            if st.button("Desativar esta frente (etapa)"):
                sb_upsert("casa_ativacoes", {"casa_id": casa_id, "etapa": etapa, "ativa": False, "ativa_em": None, "ativa_por": None}, on_conflict="casa_id,etapa")
                log_event(user["nome"], "desativar_frente", obra_id=obra_id, casa_id=casa_id, detalhes={"lote": lote, "etapa": etapa})
                st.rerun()
        else:
            st.info(f"Casa {lote} — {etapa} está INATIVA.")
            if st.button("Ativar esta frente (etapa)"):
                now = datetime.utcnow().isoformat()
                sb_upsert("casa_ativacoes", {"casa_id": casa_id, "etapa": etapa, "ativa": True, "ativa_em": now, "ativa_por": user["nome"]}, on_conflict="casa_id,etapa")
                # Semear estado_servicos para serviços desta etapa
                servs = sb_select("servicos", filters={"obra_id": obra_id, "etapa": etapa})
                for s in servs:
                    sb_upsert("estado_servicos", {"casa_id": casa_id, "servico_id": s["id"], "status": "Não iniciado", "executor": "", "data_inicio": None, "data_fim": None, "updated_at": now}, on_conflict="casa_id,servico_id")
                log_event(user["nome"], "ativar_frente", obra_id=obra_id, casa_id=casa_id, detalhes={"lote": lote, "etapa": etapa})
                st.success(f"Casa {lote} — {etapa} ativada com sucesso!")
                st.rerun()

        st.divider()
        st.subheader("Status de serviços (somente leitura)")
        servs = pd.DataFrame(sb_select("servicos", filters={"obra_id": obra_id, "etapa": etapa}, order="nome"))
        if servs.empty:
            st.info("Ainda não há serviços cadastrados para esta etapa.")
        else:
            estado = pd.DataFrame(sb_select("estado_servicos", filters={"casa_id": casa_id}))
            estado = estado.merge(servs[["id","nome"]], left_on="servico_id", right_on="id", how="right")
            estado = estado.rename(columns={"nome": "servico"})[["servico","status","executor","data_inicio","data_fim","updated_at"]]
            st.dataframe(estado, use_container_width=True)

# -------------------- Lançamentos --------------------
if page == "Lançamentos" and can_view("Lançamentos"):
    st.header("Iniciar/Finalizar Serviços")
    obras = pd.DataFrame(sb_select("obras", order="nome"))
    if obras.empty:
        st.warning("Não há obras cadastradas.")
    else:
        obra_nome = st.selectbox("Obra", obras["nome"].tolist())
        obra_id = int(obras.loc[obras["nome"]==obra_nome, "id"].iloc[0])

        etapas = pd.DataFrame(sb_select("etapas", filters={"obra_id": obra_id}, order="nome"))
        if etapas.empty:
            st.info("Cadastre etapas na Base de Dados.")
            st.stop()
        etapa = st.selectbox("Etapa", etapas["nome"].tolist(), index=0)

        # Casas ativas para a etapa
        casas = pd.DataFrame(sb_select("casas", filters={"obra_id": obra_id}, order="lote"))
        ativacoes = pd.DataFrame(sb_select("casa_ativacoes", filters={"etapa": etapa}))
        if casas.empty:
            st.info("Cadastre casas na Base de Dados.")
            st.stop()
        casas["ativa_etapa"] = casas["id"].apply(lambda cid: bool((ativacoes[ativacoes["casa_id"]==cid]["ativa"]==True).any()))
        casas_ativas = casas[casas["ativa_etapa"]]
        if casas_ativas.empty:
            st.info("Não há casas ativas para esta etapa nesta obra.")
            st.stop()
        lote = st.selectbox("Lote (Identificador)", casas_ativas["lote"].tolist())
        casa_id = int(casas_ativas.loc[casas_ativas["lote"]==lote, "id"].iloc[0])

        # Serviços da etapa
        servs = pd.DataFrame(sb_select("servicos", filters={"obra_id": obra_id, "etapa": etapa}, order="nome"))
        if servs.empty:
            st.info("Cadastre serviços para esta etapa.")
            st.stop()

        # Sugerir não concluídos
        estado = pd.DataFrame(sb_select("estado_servicos", filters={"casa_id": casa_id}))
        sugest = servs[["id","nome"]].merge(estado[["servico_id","status"]], left_on="id", right_on="servico_id", how="left")
        sugest["status"] = sugest["status"].fillna("Não iniciado")
        nao_conc = sugest[sugest["status"] != "Concluído"]

        mult_sel = st.multiselect("Selecione os serviços para INICIAR (em execução)", nao_conc["nome"].tolist())
        col_m1, col_m2, col_m3 = st.columns(3)
        executor_multi = col_m1.text_input("Executor (para todos)", value="")
        data_inicio_multi = col_m2.date_input("Data de início (para todos)", value=date.today())
        obs_multi = col_m3.text_input("Observações (opcional)", value="")

        if st.button("▶️ Iniciar serviços selecionados"):
            if not can_edit("editar_lancamentos"):
                st.error("Sem permissão para editar lançamentos.")
            elif not mult_sel:
                st.warning("Selecione pelo menos um serviço.")
            else:
                now = datetime.utcnow().isoformat()
                for nome in mult_sel:
                    sid = int(servs.loc[servs["nome"]==nome, "id"].iloc[0])
                    sb_upsert("estado_servicos", {"casa_id": casa_id, "servico_id": sid, "status": "Em execução", "executor": executor_multi or "", "data_inicio": data_inicio_multi.isoformat(), "updated_at": now}, on_conflict="casa_id,servico_id")
                    sb_insert("lancamentos", {"obra_id": obra_id, "casa_id": casa_id, "servico_id": sid, "responsavel": user["nome"], "executor": executor_multi or "", "status": "Em execução", "data_inicio": data_inicio_multi.isoformat(), "observacoes": obs_multi, "created_at": now})
                st.success(f"Iniciado(s): {len(mult_sel)} serviço(s).")
                log_event(user["nome"], "iniciar_servicos_multiplos", obra_id=obra_id, casa_id=casa_id, detalhes={"servicos": mult_sel, "executor": executor_multi, "data_inicio": data_inicio_multi.isoformat(), "obs": obs_multi})
                st.rerun()

        st.divider()
        st.subheader("Finalização de Serviço (opcional)")
        em_exec = sugest[sugest["status"] == "Em execução"]
        if em_exec.empty:
            st.info("Não há serviços em execução para finalizar.")
        else:
            servico_nome = st.selectbox("Serviço em execução", em_exec["nome"].tolist())
            servico_id = int(servs.loc[servs["nome"]==servico_nome, "id"].iloc[0])
            data_fim = st.date_input("Data de conclusão", value=date.today())
            obs = st.text_area("Observações (opcional)")
            foto = st.camera_input("Foto da conclusão (opcional)")
            foto_url = None
            if foto:
                foto_url = upload_foto(foto.getvalue(), f"{obra_id}_{casa_id}_{servico_id}_{int(datetime.utcnow().timestamp())}.jpg")

            if st.button("✅ Finalizar serviço selecionado"):
                if not can_edit("editar_lancamentos"):
                    st.error("Sem permissão para editar lançamentos.")
                else:
                    now = datetime.utcnow().isoformat()
                    sb_update("estado_servicos", {"status":"Concluído", "data_fim": data_fim.isoformat(), "updated_at": now}, {"casa_id": casa_id, "servico_id": servico_id})
                    sb_insert("lancamentos", {"obra_id": obra_id, "casa_id": casa_id, "servico_id": servico_id, "responsavel": user["nome"], "executor": "", "status": "Concluído", "data_conclusao": data_fim.isoformat(), "observacoes": obs, "foto_path": foto_url, "created_at": now})
                    st.success(f"Serviço '{servico_nome}' finalizado.")
                    log_event(user["nome"], "finalizar_servico", obra_id=obra_id, casa_id=casa_id, servico_id=servico_id, detalhes={"servico": servico_nome, "data_fim": data_fim.isoformat(), "obs": obs})
                    st.rerun()

        st.divider()
        st.subheader("Estado atual dos serviços desta casa/etapa")
        estado = pd.DataFrame(sb_select("estado_servicos", filters={"casa_id": casa_id}))
        estado = estado.merge(servs[["id","nome"]], left_on="servico_id", right_on="id", how="right")
        estado = estado.rename(columns={"nome":"servico"})[["servico","status","executor","data_inicio","data_fim","updated_at"]]
        st.dataframe(estado, use_container_width=True)

# -------------------- Dashboard --------------------
if page == "Dashboard":
    st.header("Dashboard (visão por CASA)")
    obras = pd.DataFrame(sb_select("obras", order="nome"))
    if obras.empty:
        st.info("Nenhuma obra cadastrada.")
        st.stop()
    col_f1, col_f2 = st.columns(2)
    obra_sel = col_f1.selectbox("Obra", obras["nome"].tolist())
    obra_id = int(obras.loc[obras["nome"]==obra_sel, "id"].iloc[0])
    etapas = pd.DataFrame(sb_select("etapas", filters={"obra_id": obra_id}, order="nome"))
    etapa_opts = ["Todas"] + (etapas["nome"].tolist() if not etapas.empty else [])
    etapa_sel = col_f2.selectbox("Etapa", etapa_opts, index=0)

    casas = pd.DataFrame(sb_select("casas", filters={"obra_id": obra_id}, order="lote"))
    if casas.empty:
        st.info("Não há casas para esta obra.")
        st.stop()
    casa_ids = casas["id"].tolist()

    ativacoes = pd.DataFrame(sb_select("casa_ativacoes"))
    if etapa_sel == "Todas":
        casas["ativa_etapa"] = casas["id"].apply(lambda cid: bool((ativacoes[ativacoes["casa_id"]==cid]["ativa"]==True).any()))
    else:
        casas["ativa_etapa"] = casas["id"].apply(lambda cid: bool((ativacoes[(ativacoes["casa_id"]==cid) & (ativacoes["etapa"]==etapa_sel)]["ativa"]==True).any()))

    estado = pd.DataFrame(sb_select("estado_servicos", filters=None))
    servs = pd.DataFrame(sb_select("servicos", filters={"obra_id": obra_id}))
    if etapa_sel != "Todas":
        servs = servs[servs["etapa"] == etapa_sel]
    total_count = len(servs)

    if not estado.empty:
        estado = estado.merge(servs[["id","etapa"]], left_on="servico_id", right_on="id", how="inner")
        if etapa_sel != "Todas":
            estado = estado[estado["etapa"] == etapa_sel]

    exec_por_casa = estado[estado["status"] == "Em execução"].groupby("casa_id")["servico_id"].nunique().rename("em_exec") if not estado.empty else pd.Series(dtype=int)
    concl_por_casa = estado[estado["status"] == "Concluído"].groupby("casa_id")["servico_id"].nunique().rename("concluidos") if not estado.empty else pd.Series(dtype=int)

    resumo = casas.copy()
    resumo["total_serv"] = total_count
    resumo = resumo.merge(concl_por_casa, left_on="id", right_index=True, how="left")
    resumo = resumo.merge(exec_por_casa, left_on="id", right_index=True, how="left")
    resumo["concluidos"] = resumo["concluidos"].fillna(0).astype(int)
    resumo["em_exec"] = resumo["em_exec"].fillna(0).astype(int)

    def classifica(row):
        if not row["ativa_etapa"]:
            return "Não iniciado"
        if total_count > 0 and row["concluidos"] == total_count:
            return "Concluído"
        return "Em execução"

    resumo["status_casa"] = resumo.apply(classifica, axis=1)
    resumo["progresso_%"] = ((resumo["concluidos"] + resumo["em_exec"]) / total_count * 100 if total_count > 0 else 0)
    resumo["progresso_%"] = resumo["progresso_%"].fillna(0).round(1)
    resumo = resumo.rename(columns={"lote":"Lote"})

    c1, c2, c3 = st.columns(3)
    c1.metric("Casas — Não iniciado", int((resumo["status_casa"] == "Não iniciado").sum()))
    c2.metric("Casas — Em execução", int((resumo["status_casa"] == "Em execução").sum()))
    c3.metric("Casas — Concluídas (100%)", int((resumo["status_casa"] == "Concluído").sum()))

    st.divider()
    st.dataframe(resumo[["Lote","status_casa","progresso_%"]], use_container_width=True, hide_index=True)

# -------------------- Observações --------------------
if page == "Observações":
    st.header("Observações por Casa")
    st.caption("Veja (e exporte) todas as observações lançadas em Início e Finalização.")

    obras = pd.DataFrame(sb_select("obras", order="nome"))
    if obras.empty:
        st.info("Não há obras cadastradas.")
        st.stop()
    obra_sel = st.selectbox("Obra", obras["nome"].tolist(), key="obs_ob")
    obra_id = int(obras.loc[obras["nome"]==obra_sel, "id"].iloc[0])

    casas = pd.DataFrame(sb_select("casas", filters={"obra_id": obra_id}, order="lote"))
    if casas.empty:
        st.info("Não há casas nesta obra.")
        st.stop()
    lote_sel = st.selectbox("Casa (lote)", casas["lote"].tolist(), key="obs_lote")
    casa_id = int(casas.loc[casas["lote"]==lote_sel, "id"].iloc[0])

    etapas = pd.DataFrame(sb_select("servicos", filters={"obra_id": obra_id}, select="etapa")).dropna()
    etapa_opts = ["Todas"] + sorted(set([e["etapa"] for e in etapas.to_dict(orient="records")])) if not etapas.empty else ["Todas"]
    etapa_sel = st.selectbox("Etapa (opcional)", etapa_opts, key="obs_et")

    sql_rows = sb_select("lancamentos", filters={"obra_id": obra_id, "casa_id": casa_id})
    df = pd.DataFrame(sql_rows)
    if not df.empty:
        df = df[df["anulado"] == False]
        df = df[df["observacoes"].fillna("").str.strip() != ""]
        servs = pd.DataFrame(sb_select("servicos", filters={"obra_id": obra_id}))
        df = df.merge(servs[["id","nome","etapa"]], left_on="servico_id", right_on="id", how="left")
        df = df.rename(columns={"created_at":"data","nome":"servico"})[["data","etapa","servico","status","executor","data_inicio","data_conclusao","observacoes","responsavel"]]
        if etapa_sel != "Todas":
            df = df[df["etapa"] == etapa_sel]

    st.write(f"Observações encontradas: **{0 if df.empty else len(df)}**")
    st.dataframe(df if not df.empty else pd.DataFrame(), use_container_width=True, hide_index=True)

    if not df.empty:
        csv_bytes = df.to_csv(index=False).encode("utf-8-sig")
        st.download_button("Baixar CSV das observações", data=csv_bytes, file_name=f"observacoes_{obra_sel}_{lote_sel}.csv", mime="text/csv")
