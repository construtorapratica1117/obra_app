import os
import json
import sqlite3
import pandas as pd
import streamlit as st
from datetime import datetime, date

APP_DIR = os.path.dirname(__file__)
DB_PATH = os.path.join(APP_DIR, "db.sqlite3")
USERS_JSON = os.path.join(APP_DIR, "usuarios.json")
UPLOAD_DIR = os.path.join(APP_DIR, "uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)

st.set_page_config(page_title="Acompanhamento de Obras", page_icon="🏗️", layout="wide")

# -------------------- Auth (JSON) --------------------
def load_users():
    try:
        with open(USERS_JSON, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"users": []}

def save_users(data):
    with open(USERS_JSON, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def check_login(username, password):
    data = load_users()
    for u in data.get("users", []):
        if u.get("username") == username and u.get("password") == password and u.get("ativo", True):
            return {"username": u["username"], "nome": u["nome"], "role": u.get("role", "user")}
    return None

def require_login():
    return "user" in st.session_state and st.session_state["user"]

# -------------------- Permissions --------------------
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
    # Aceita dict ou pandas.Series
    if hasattr(user_dict, "to_dict"):
        user_dict = user_dict.to_dict()
    role = user_dict.get("role", "user") if isinstance(user_dict, dict) else "user"
    p = dict(_default_permissoes(role))
    up_raw = (user_dict.get("permissoes") if isinstance(user_dict, dict) else None)
    # Se vier NaN/None/str/float, zera para {}
    up = up_raw if isinstance(up_raw, dict) else {}
    p.update({k: bool(up.get(k, p[k])) for k in p})
    return p

def can_view(page_name):
    user = st.session_state.get("user", {})
    p = _merge_permissoes(user)
    mapping = {
        "Ativar Casa": "ver_ativar_casa",
        "Lançamentos": "ver_lancamentos",
        "Dashboard": "ver_dashboard",
        "Base de Dados": "ver_servicos",
        "Logs": "ver_logs",
        "Admin": "ver_admin",
        "Minha Conta": True,
    }
    key = mapping.get(page_name, True)
    return p.get(key, True) if isinstance(key, str) else bool(key)

def can_edit(action_key):
    user = st.session_state.get("user", {})
    p = _merge_permissoes(user)
    return bool(p.get(action_key, False))



# -------------------- DB helpers --------------------
def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def init_db(conn):
    cur = conn.cursor()
    cur.execute("PRAGMA foreign_keys = ON;")


    # [AUTO-PATCH] Tabela 'etapas' e coluna obra_id em servicos
    cur.execute("""CREATE TABLE IF NOT EXISTS etapas (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        obra_id INTEGER NOT NULL,
        nome TEXT NOT NULL,
        UNIQUE (obra_id, nome),
        FOREIGN KEY (obra_id) REFERENCES obras(id)
    )""")
    # adicionar obra_id em servicos se faltar
    cols = [r[1] for r in cur.execute("PRAGMA table_info(servicos)")]
    if "obra_id" not in cols:
        try:
            cur.execute("ALTER TABLE servicos ADD COLUMN obra_id INTEGER REFERENCES obras(id)")
        except Exception as _e:
            pass
        # Tabelas principais
    cur.execute("CREATE TABLE IF NOT EXISTS obras (id INTEGER PRIMARY KEY AUTOINCREMENT, nome TEXT UNIQUE NOT NULL)")
    cur.execute("""CREATE TABLE IF NOT EXISTS casas (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        obra_id INTEGER NOT NULL,
        lote TEXT NOT NULL,
        cod_tipologia TEXT,
        tipologia TEXT,
        ativa INTEGER NOT NULL DEFAULT 0,
        ativa_em TEXT,
        ativa_por TEXT,
        UNIQUE(obra_id, lote),
        FOREIGN KEY (obra_id) REFERENCES obras(id) ON DELETE CASCADE
    )""")
    # Tabela de ativações por etapa (frente de serviço)
    cur.execute("""CREATE TABLE IF NOT EXISTS casa_ativacoes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        casa_id INTEGER NOT NULL,
        etapa TEXT CHECK(etapa IN ('Reboco','Pintura','Revestimento')) NOT NULL,
        ativa INTEGER NOT NULL DEFAULT 0,
        ativa_em TEXT,
        ativa_por TEXT,
        UNIQUE(casa_id, etapa),
        FOREIGN KEY (casa_id) REFERENCES casas(id) ON DELETE CASCADE
    )""")

    cur.execute("CREATE TABLE IF NOT EXISTS servicos (id INTEGER PRIMARY KEY AUTOINCREMENT, nome TEXT NOT NULL, etapa TEXT NOT NULL, UNIQUE(nome, etapa))")
    cur.execute("""CREATE TABLE IF NOT EXISTS previstos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        casa_id INTEGER NOT NULL,
        servico_id INTEGER NOT NULL,
        metragem REAL NOT NULL,
        UNIQUE(casa_id, servico_id),
        FOREIGN KEY (casa_id) REFERENCES casas(id) ON DELETE CASCADE,
        FOREIGN KEY (servico_id) REFERENCES servicos(id) ON DELETE CASCADE
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS estado_servicos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        casa_id INTEGER NOT NULL,
        servico_id INTEGER NOT NULL,
        status TEXT CHECK(status IN ('Não iniciado','Em execução','Concluído')) DEFAULT 'Não iniciado',
        executor TEXT,
        data_inicio TEXT,
        data_fim TEXT,
        updated_at TEXT,
        UNIQUE(casa_id, servico_id),
        FOREIGN KEY (casa_id) REFERENCES casas(id) ON DELETE CASCADE,
        FOREIGN KEY (servico_id) REFERENCES servicos(id) ON DELETE CASCADE
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS lancamentos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        obra_id INTEGER NOT NULL,
        casa_id INTEGER NOT NULL,
        servico_id INTEGER NOT NULL,
        responsavel TEXT,
        executor TEXT,
        status TEXT CHECK(status IN ('Não iniciado','Em execução','Concluído')),
        data_inicio TEXT,
        data_conclusao TEXT,
        observacoes TEXT,
        foto_path TEXT,
        created_at TEXT NOT NULL,
        FOREIGN KEY (obra_id) REFERENCES obras(id) ON DELETE CASCADE,
        FOREIGN KEY (casa_id) REFERENCES casas(id) ON DELETE CASCADE,
        FOREIGN KEY (servico_id) REFERENCES servicos(id) ON DELETE CASCADE
    )""")

    pass


    # Migrations idempotentes
    try:
        cur.execute("ALTER TABLE casas ADD COLUMN ativa INTEGER NOT NULL DEFAULT 0")
    except sqlite3.OperationalError:
        pass
    try:
        cur.execute("ALTER TABLE casas ADD COLUMN ativa_em TEXT")
    except sqlite3.OperationalError:
        pass
    try:
        cur.execute("ALTER TABLE casas ADD COLUMN ativa_por TEXT")
    except sqlite3.OperationalError:
        pass
    try:
        cur.execute("ALTER TABLE lancamentos ADD COLUMN executor TEXT")
    except sqlite3.OperationalError:
        pass

    # Migrations: campos de anulação em lancamentos
    try:
        cur.execute("ALTER TABLE lancamentos ADD COLUMN anulado INTEGER NOT NULL DEFAULT 0")
    except sqlite3.OperationalError:
        pass
    try:
        cur.execute("ALTER TABLE lancamentos ADD COLUMN anulado_por TEXT")
    except sqlite3.OperationalError:
        pass
    try:
        cur.execute("ALTER TABLE lancamentos ADD COLUMN anulado_em TEXT")
    except sqlite3.OperationalError:
        pass
    try:
        cur.execute("ALTER TABLE lancamentos ADD COLUMN anulacao_motivo TEXT")
    except sqlite3.OperationalError:
        pass


    # Auditoria fora de try/except de migrations
    cur.execute("""CREATE TABLE IF NOT EXISTS auditoria (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT NOT NULL,
        usuario TEXT,
        acao TEXT NOT NULL,
        obra_id INTEGER,
        casa_id INTEGER,
        servico_id INTEGER,
        detalhes TEXT
    )""")

    conn.commit()


def one(conn, sql, args=()):

    cur = conn.execute(sql, args)
    row = cur.fetchone()
    return row[0] if row else None

def get_previsto_df(conn):
    sql = """
    SELECT o.nome as obra, s.etapa, s.nome as servico, c.lote, c.tipologia, p.metragem,
           c.id as casa_id, s.id as servico_id, c.ativa, c.ativa_em
    FROM previstos p
    JOIN casas c ON c.id = p.casa_id
    JOIN servicos s ON s.id = p.servico_id
    JOIN obras o ON o.id = c.obra_id
    """
    return pd.read_sql_query(sql, conn)

def get_estado_df(conn):
    sql = """
    SELECT es.casa_id, es.servico_id, es.status, es.executor, es.data_inicio, es.data_fim, es.updated_at
    FROM estado_servicos es
    """
    return pd.read_sql_query(sql, conn)


def log_event(conn, usuario, acao, obra_id=None, casa_id=None, servico_id=None, detalhes=None):
    ts = datetime.now().isoformat()
    det = json.dumps(detalhes, ensure_ascii=False) if detalhes is not None else None
    conn.execute(
        "INSERT INTO auditoria (timestamp, usuario, acao, obra_id, casa_id, servico_id, detalhes) VALUES (?,?,?,?,?,?,?)",
        (ts, usuario, acao, obra_id, casa_id, servico_id, det)
    )
    conn.commit()

# -------------------- UI --------------------
conn = get_conn()
init_db(conn)

st.sidebar.title("🏗️ Acompanhamento de Obras — Login")

# Login gate
if not require_login():
    with st.form("login"):
        st.subheader("Entrar")
        u = st.text_input("Usuário")
        p = st.text_input("Senha", type="password")
        ok = st.form_submit_button("Entrar")
        if ok:
            data = load_users()
            user = None
            for us in data.get("users", []):
                if us.get("username")==u and us.get("password")==p and us.get("ativo", True):
                    perms = us.get("permissoes") or _default_permissoes(us.get("role","user"))
                    user = {"username": u, "nome": us.get("nome", u), "role": us.get("role","user"), "permissoes": perms}
                    break
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

pages_all = ["Ativar Casa", "Lançamentos", "Dashboard", "Base de Dados", "Logs", "Correções", "Admin", "Minha Conta"]
pages = [p for p in pages_all if can_view(p)]
page = st.sidebar.radio("Navegação", pages)

# -------- Minha Conta --------


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
                data = load_users()
                uname = user.get("username")
                users = data.get("users", [])
                found = False
                for u in users:
                    if u.get("username") == uname and u.get("ativo", True):
                        if u.get("password") != current_pw:
                            st.error("Senha atual incorreta.")
                            break
                        u["password"] = new_pw
                        save_users({"users": users})
                        st.success("Senha alterada com sucesso.")
                        log_event(conn, user["nome"], "alterar_senha", detalhes={"username": uname})
                        found = True
                        break
                if not found:
                    st.error("Usuário não encontrado ou inativo.")

# -------- Serviços (Admin) --------


if page == "Base de Dados" and can_view("Base de Dados"):
    st.header("Base de Dados")
    tabs = st.tabs(["Obras", "Etapas", "Base de Dados"])

    # --- Obras ---
    with tabs[0]:
        st.subheader("Obras")
        with st.form("nova_obra"):
            nome_obra = st.text_input("Nome da Obra", placeholder="Ex.: Berlin")
            ok = st.form_submit_button("Criar Obra")
            if ok and nome_obra.strip():
                try:
                    conn.execute("INSERT INTO obras (nome) VALUES (?)", (nome_obra.strip(),))
                    conn.commit()
                    st.success(f"Obra '{nome_obra}' criada.")
                except Exception as e:
                    st.error(f"Não foi possível criar a obra: {e}")
        obras_df = pd.read_sql_query("SELECT id, nome FROM obras ORDER BY nome", conn)
        st.dataframe(obras_df, use_container_width=True, hide_index=True)

    # --- Etapas ---
    with tabs[1]:
        st.subheader("Etapas por Obra")
        obras_df = pd.read_sql_query("SELECT id, nome FROM obras ORDER BY nome", conn)
        if obras_df.empty:
            st.info("Crie uma obra primeiro.")
        else:
            obra_sel = st.selectbox("Obra", obras_df["nome"].tolist(), key="bd_et_ob")
            obra_id = int(obras_df.loc[obras_df["nome"] == obra_sel, "id"].iloc[0])
            with st.form("nova_etapa"):
                etapa_nome = st.text_input("Nova Etapa", placeholder="Ex.: Reboco")
                ok_e = st.form_submit_button("Adicionar Etapa")
                if ok_e and etapa_nome.strip():
                    try:
                        conn.execute("INSERT INTO etapas (obra_id, nome) VALUES (?, ?)", (obra_id, etapa_nome.strip()))
                        conn.commit()
                        st.success(f"Etapa '{etapa_nome}' criada para {obra_sel}.")
                    except Exception as e:
                        st.error(f"Não foi possível criar a etapa: {e}")
            etapas_df = pd.read_sql_query("SELECT id, nome FROM etapas WHERE obra_id=? ORDER BY nome", conn, params=(obra_id,))
            st.dataframe(etapas_df, use_container_width=True, hide_index=True)

    # --- Serviços ---
    with tabs[2]:
        st.subheader("Serviços por Etapa")
        obras_df = pd.read_sql_query("SELECT id, nome FROM obras ORDER BY nome", conn)
        if obras_df.empty:
            st.info("Crie uma obra primeiro.")
        else:
            obra_sel = st.selectbox("Obra", obras_df["nome"].tolist(), key="bd_sv_ob")
            obra_id = int(obras_df.loc[obras_df["nome"] == obra_sel, "id"].iloc[0])
            etapas_df = pd.read_sql_query("SELECT id, nome FROM etapas WHERE obra_id=? ORDER BY nome", conn, params=(obra_id,))
            # fallback: usar etapas distintas de servicos quando ainda não existir cadastro explícito
            if etapas_df.empty:
                etapas_df = pd.read_sql_query("SELECT NULL as id, etapa as nome FROM servicos WHERE obra_id=? GROUP BY etapa ORDER BY etapa", conn, params=(obra_id,))
            if etapas_df.empty:
                st.info("Cadastre uma etapa para esta obra.")
            else:
                etapa_sel = st.selectbox("Etapa", etapas_df["nome"].tolist(), key="bd_sv_et")
                with st.form("novo_servico"):
                    nome_serv = st.text_input("Nome do Serviço", placeholder="Ex.: Chap. Ext. + Tela")
                    ok_s = st.form_submit_button("Adicionar Serviço")
                    if ok_s and nome_serv.strip():
                        try:
                            conn.execute("INSERT INTO servicos (nome, etapa, obra_id) VALUES (?, ?, ?)", (nome_serv.strip(), etapa_sel, obra_id))
                            conn.commit()
                            st.success(f"Serviço '{nome_serv}' adicionado em {etapa_sel} ({obra_sel}).")
                        except Exception as e:
                            st.error(f"Não foi possível adicionar o serviço: {e}")
                servs_df = pd.read_sql_query("SELECT id, nome FROM servicos WHERE (obra_id=? OR obra_id IS NULL) AND etapa=? ORDER BY nome", conn, params=(obra_id, etapa_sel))
                st.dataframe(servs_df, use_container_width=True, hide_index=True)

# -------- Logs --------
if page == "Logs" and can_view("Logs"):
    st.header("Logs do Sistema")
    st.caption("Registro de tudo que foi feito: quem, quando e o que.")

    # Filtros simples
    col1, col2, col3 = st.columns(3)
    usuarios = pd.read_sql_query("SELECT DISTINCT COALESCE(usuario,'') as usuario FROM auditoria ORDER BY usuario", conn)
    usuario_sel = col1.selectbox("Usuário", ["Todos"] + [u for u in usuarios["usuario"].tolist() if u])

    acoes = pd.read_sql_query("SELECT DISTINCT acao FROM auditoria ORDER BY acao", conn)
    acao_sel = col2.selectbox("Ação", ["Todas"] + acoes["acao"].dropna().tolist())

    limite = col3.number_input("Mostrar últimos (registros)", min_value=50, max_value=100000, value=200, step=50)

    base_sql = "SELECT timestamp, usuario, acao, obra_id, casa_id, servico_id, detalhes FROM auditoria"
    where = []
    params = []

    if usuario_sel != "Todos":
        where.append("usuario = ?")
        params.append(usuario_sel)
    if acao_sel != "Todas":
        where.append("acao = ?")
        params.append(acao_sel)

    if where:
        base_sql += " WHERE " + " AND ".join(where)
    base_sql += " ORDER BY id DESC LIMIT ?"
    params.append(int(limite))

    df_logs = pd.read_sql_query(base_sql, conn, params=params)
    st.dataframe(df_logs, use_container_width=True)

    # Exportar
    from io import BytesIO
    if st.button("Exportar CSV"):
        csv_bytes = df_logs.to_csv(index=False).encode("utf-8-sig")
        st.download_button("Baixar CSV", data=csv_bytes, file_name="logs.csv", mime="text/csv")

# -------- Admin --------


if page == "Admin" and can_view("Admin") and can_edit("editar_usuarios"):
    st.header("Administração de Usuários")
    data = load_users()

    st.subheader("Usuários cadastrados")
    if len(data.get("users", [])) == 0:
        st.info("Nenhum usuário no arquivo usuarios.json")
    else:
        df_users = pd.DataFrame(data["users"])
        # Mostrar permissões resumidas (só para visualização)
        def _perm_resumo(u):
            p = _merge_permissoes(u.to_dict() if hasattr(u, "to_dict") else u)
            chaves = ["ver_ativar_casa","ver_lancamentos","ver_dashboard","ver_servicos","ver_logs","ver_admin",
                      "editar_lancamentos","editar_servicos","editar_usuarios"]
            return ", ".join([k for k in chaves if p.get(k)])
        df_users["permissoes_resumo"] = df_users.apply(_perm_resumo, axis=1)
        cols_show = [c for c in ["username","nome","role","ativo","permissoes_resumo"] if c in df_users.columns]
        st.dataframe(df_users[cols_show], use_container_width=True)

    st.divider()
    st.subheader("Criar usuário")
    with st.form("new_user"):
        nu_user = st.text_input("Usuário (login)")
        nu_nome = st.text_input("Nome completo")
        nu_role = st.selectbox("Perfil", ["user", "admin"])
        nu_pw = st.text_input("Senha inicial", type="password", value="123456")
        # Permissões iniciais
        base_p = _default_permissoes(nu_role)
        c1, c2, c3 = st.columns(3)
        with c1:
            v_ac = st.checkbox("Ver Ativar Casa", value=base_p["ver_ativar_casa"])
            v_la = st.checkbox("Ver Lançamentos", value=base_p["ver_lancamentos"])
            v_da = st.checkbox("Ver Dashboard", value=base_p["ver_dashboard"])
        with c2:
            v_se = st.checkbox("Ver Serviços", value=base_p["ver_servicos"])
            v_lo = st.checkbox("Ver Logs", value=base_p["ver_logs"])
            v_ad = st.checkbox("Ver Admin", value=base_p["ver_admin"])
            with c3:
                e_la = st.checkbox("Editar Lançamentos", value=base_p["editar_lancamentos"])
                e_se = st.checkbox("Editar Serviços", value=base_p["editar_servicos"])
                e_us = st.checkbox("Editar Usuários", value=base_p["editar_usuarios"])
                c_rr = st.checkbox("Corrigir Registros (Correções)", value=base_p.get("corrigir_registros", False))



        ok_new = st.form_submit_button("Criar")
        if ok_new:
            users = data.get("users", [])
            if any(u["username"] == nu_user for u in users):
                st.error("Usuário já existe.")
            else:
                perms = {
                    "ver_ativar_casa": v_ac, "ver_lancamentos": v_la, "ver_dashboard": v_da,
                    "ver_servicos": v_se, "ver_logs": v_lo, "ver_admin": v_ad,
                    "editar_lancamentos": e_la, "editar_servicos": e_se, "editar_usuarios": e_us, "corrigir_registros": c_rr,
                }
                users.append({
                    "username": nu_user, "nome": nu_nome, "role": nu_role,
                    "password": nu_pw, "ativo": True, "permissoes": perms
                })
                data["users"] = users
                save_users(data)
                st.success("Usuário criado. Atualize a página para ver na lista.")
                log_event(conn, st.session_state["user"]["nome"], "criar_usuario", detalhes={"username": nu_user, "nome": nu_nome, "role": nu_role, "permissoes": perms})

    st.divider()
    st.subheader("Editar usuário existente")
    users = data.get("users", [])
    if users:
        opts = {f"{u['nome']} ({u['username']})": i for i,u in enumerate(users)}
        sel = st.selectbox("Selecione um usuário", list(opts.keys()))
        idx = opts[sel]
        u = users[idx]
        with st.form("edit_user"):
            eu_nome = st.text_input("Nome completo", value=u.get("nome",""))
            eu_role = st.selectbox("Perfil", ["user","admin"], index=0 if u.get("role")=="user" else 1)
            eu_status = st.selectbox("Status", ["Ativo","Inativo"], index=0 if u.get("ativo", True) else 1)
            eu_pw_reset = st.checkbox("Resetar senha para 123456")
            # Permissões
            base_p = _merge_permissoes(u | {"role": eu_role}) if hasattr(dict, "__or__") else _merge_permissoes({"role": eu_role, "permissoes": u.get("permissoes")})
            c1, c2, c3 = st.columns(3)
            with c1:
                v_ac = st.checkbox("Ver Ativar Casa", value=base_p["ver_ativar_casa"])
                v_la = st.checkbox("Ver Lançamentos", value=base_p["ver_lancamentos"])
                v_da = st.checkbox("Ver Dashboard", value=base_p["ver_dashboard"])
            with c2:
                v_se = st.checkbox("Ver Serviços", value=base_p["ver_servicos"])
                v_lo = st.checkbox("Ver Logs", value=base_p["ver_logs"])
                v_ad = st.checkbox("Ver Admin", value=base_p["ver_admin"])
            with c3:
                e_la = st.checkbox("Editar Lançamentos", value=base_p["editar_lancamentos"])
                e_se = st.checkbox("Editar Serviços", value=base_p["editar_servicos"])
                e_us = st.checkbox("Editar Usuários", value=base_p["editar_usuarios"])
                c_rr = st.checkbox("Corrigir Registros (Correções)", value=base_p.get("corrigir_registros", False))


            ok_m = st.form_submit_button("Salvar alterações")

            if ok_m:
                u["nome"] = eu_nome
                u["role"] = eu_role
                u["ativo"] = (eu_status == "Ativo")
                if eu_pw_reset:
                    u["password"] = "123456"
                u["permissoes"] = {
                    "ver_ativar_casa": v_ac, "ver_lancamentos": v_la, "ver_dashboard": v_da,
                    "ver_servicos": v_se, "ver_logs": v_lo, "ver_admin": v_ad,
                    "editar_lancamentos": e_la, "editar_servicos": e_se, "editar_usuarios": e_us, "corrigir_registros": c_rr,
                }
                data["users"][idx] = u
                save_users(data)
                st.success("Alterações salvas.")
                log_event(conn, st.session_state["user"]["nome"], "alterar_usuario", detalhes={"username": u["username"], "role": eu_role, "ativo": u["ativo"], "permissoes": u["permissoes"]})

    st.divider()
    st.subheader("Resetar Lançamentos por Casa")
    st.caption("Apenas casas **ativas** aparecem aqui. O reset apaga todos os lançamentos dessa casa e zera o estado dos serviços para *Não iniciado*.")
    obras_rst = pd.read_sql_query("SELECT * FROM obras ORDER BY nome", conn)
    if obras_rst.empty:
        st.info("Não há obras cadastradas.")
    else:
        obra_nome_rst = st.selectbox("Obra", obras_rst["nome"].tolist(), key="rst_ob")
        obra_id_rst = int(pd.read_sql_query("SELECT id FROM obras WHERE nome=?", conn, params=(obra_nome_rst,)).iloc[0,0])
        casas_ativas = pd.read_sql_query("""
            SELECT c.id, c.lote
            FROM casas c
            WHERE c.obra_id = ?
              AND (
                    c.ativa = 1
                 OR EXISTS (SELECT 1 FROM casa_ativacoes ca WHERE ca.casa_id = c.id AND ca.ativa = 1)
              )
            ORDER BY c.lote
        """, conn, params=(obra_id_rst,))
        if casas_ativas.empty:
            st.info("Não há casas **ativas** nesta obra.")
        else:
            lote_opts = casas_ativas["lote"].tolist()
            lote_sel_rst = st.selectbox("Casa (lote) — apenas ativas", lote_opts, key="rst_lote")
            casa_id_rst = int(casas_ativas.loc[casas_ativas["lote"] == lote_sel_rst, "id"].iloc[0])

            with st.expander("⚠️ Confirmar reset desta casa", expanded=False):
                colr1, colr2 = st.columns([1,2])
                confirmar = colr1.checkbox("Estou ciente e desejo resetar esta casa", key="rst_chk")
                txtconf = colr2.text_input('Digite "RESETAR" para confirmar', value="", key="rst_txt")
                btn = st.button("🧹 Resetar lançamentos da casa", type="primary", disabled=not (confirmar and txtconf.strip().upper() == "RESETAR"), key="rst_btn")
                
                if btn:
                    try:
                        now = datetime.now().isoformat()
                        # Apaga lançamentos da casa
                        conn.execute("DELETE FROM lancamentos WHERE casa_id=?", (casa_id_rst,))
                        # Zera estado dos serviços da casa
                        conn.execute(
                            "UPDATE estado_servicos SET status='Não iniciado', executor=NULL, data_inicio=NULL, data_fim=NULL, updated_at=? WHERE casa_id=?",
                            (now, casa_id_rst)
                        )
                        # Desativa a casa (exige reativação antes de novos lançamentos)
                        conn.execute("UPDATE casas SET ativa=0, ativa_em=NULL, ativa_por=NULL WHERE id=?", (casa_id_rst,))
                        conn.commit()
                        st.success(f"Lançamentos da casa {lote_sel_rst} resetados e a casa foi desativada.")
                        log_event(conn, st.session_state["user"]["nome"], "resetar_lancamentos_casa", obra_id=obra_id_rst, casa_id=casa_id_rst, detalhes={"lote": lote_sel_rst, "desativou_casa": True})
                        st.rerun()
                    except Exception as e:
                        st.error(f"Falha ao resetar: {e}")
# -------- Correções --------
if page == "Correções" and can_view("Admin") and can_edit("corrigir_registros"):
    st.header("Correções (Desfazer/Editar Registros)")
    st.caption("Somente casas com lançamentos ativos (não anulados) aparecem aqui. Todas as alterações são auditadas.")

    # 1) Selecionar obra
    obras = pd.read_sql_query("SELECT * FROM obras ORDER BY nome", conn)
    if obras.empty:
        st.info("Não há obras cadastradas.")
    else:
        obra_nome = st.selectbox("Obra", obras["nome"].tolist(), key="cor_ob")
        obra_id = int(pd.read_sql_query("SELECT id FROM obras WHERE nome=?", conn, params=(obra_nome,)).iloc[0,0])

        # 2) Listar casas desta obra que possuem lançamentos não anulados
        casas_lanc = pd.read_sql_query(
            """
            SELECT DISTINCT c.id as casa_id, c.lote, c.ativa, c.ativa_em, c.ativa_por
            FROM lancamentos l
            JOIN casas c ON c.id = l.casa_id
            WHERE COALESCE(l.anulado,0)=0 AND c.obra_id = ?
            ORDER BY c.lote
            """, conn, params=(obra_id,)
        )
        if casas_lanc.empty:
            st.info("Não há casas com lançamentos nesta obra.")
        else:
            # Pequeno resumo tipo dashboard
            st.subheader("Casas com lançamentos")
            st.dataframe(casas_lanc[["lote","ativa","ativa_em","ativa_por"]], use_container_width=True, hide_index=True)

            # 3) Selecionar casa (lote)
            lote_sel = st.selectbox("Selecionar casa (lote) para corrigir", casas_lanc["lote"].tolist(), key="cor_lote")
            casa_id = int(casas_lanc.loc[casas_lanc["lote"] == lote_sel, "casa_id"].iloc[0])

            # 4) Serviços dessa casa com lançamentos (não anulados)
            servs_casa = pd.read_sql_query(
                """
                SELECT DISTINCT s.id as servico_id, s.nome as servico, s.etapa
                FROM lancamentos l
                JOIN servicos s ON s.id = l.servico_id
                WHERE COALESCE(l.anulado,0)=0 AND l.casa_id = ?
                ORDER BY s.etapa, s.nome
                """, conn, params=(casa_id,)
            )
            if servs_casa.empty:
                st.info("Esta casa não possui lançamentos ativos.")
            else:
                col_f1, col_f2 = st.columns(2)
                etapa_opts = ["Todas"] + sorted(servs_casa["etapa"].unique().tolist())
                etapa_sel = col_f1.selectbox("Etapa", etapa_opts, key="cor_etp")
                if etapa_sel != "Todas":
                    servs_f = servs_casa[servs_casa["etapa"] == etapa_sel]
                else:
                    servs_f = servs_casa
                serv_nome = col_f2.selectbox("Serviço", servs_f["servico"].tolist(), key="cor_srv")
                servico_id = int(servs_f.loc[servs_f["servico"] == serv_nome, "servico_id"].iloc[0])

                st.divider()

                # 5) Quadro do estado atual + últimos lançamentos
                st.subheader(f"Estado atual — Casa {lote_sel} • {serv_nome} ({etapa_sel if etapa_sel!='Todas' else servs_casa.loc[servs_casa['servico']==serv_nome, 'etapa'].iloc[0]})")

                es = pd.read_sql_query(
                    "SELECT status, executor, data_inicio, data_fim, updated_at FROM estado_servicos WHERE casa_id=? AND servico_id=?",
                    conn, params=(casa_id, servico_id)
                )
                if es.empty:
                    st.info("Ainda não há estado salvo para este serviço; pode existir apenas lançamento pontual.")
                    cur_status = "Não iniciado"; cur_exec=""; cur_ini=None; cur_fim=None
                else:
                    cur_status = es["status"].iloc[0] or "Não iniciado"
                    cur_exec = es["executor"].iloc[0] or ""
                    cur_ini = es["data_inicio"].iloc[0]
                    cur_fim = es["data_fim"].iloc[0]

                # Últimos 5 lançamentos do serviço
                ult = pd.read_sql_query(
                    """
                    SELECT id, status, responsavel, executor, data_inicio, data_conclusao, observacoes, created_at
                    FROM lancamentos
                    WHERE COALESCE(anulado,0)=0 AND casa_id=? AND servico_id=?
                    ORDER BY created_at DESC
                    LIMIT 5
                    """, conn, params=(casa_id, servico_id)
                )
                st.write("**Últimos lançamentos (ativos)**")
                st.dataframe(ult, use_container_width=True)

                st.divider()

                # 6) Ações de correção em duas colunas
                ca, cb = st.columns(2)

                with ca:
                    st.markdown("### Anular último lançamento do serviço")
                    motivo_anul = st.text_input("Motivo da anulação (obrigatório)", key="cor_mot_anul")
                    if st.button("Anular último lançamento", key="btn_anul"):
                        if not motivo_anul.strip():
                            st.error("Informe o motivo.")
                        else:
                            row = pd.read_sql_query(
                                "SELECT id FROM lancamentos WHERE COALESCE(anulado,0)=0 AND casa_id=? AND servico_id=? ORDER BY created_at DESC LIMIT 1",
                                conn, params=(casa_id, servico_id)
                            )
                            if row.empty:
                                st.info("Não há lançamentos para anular.")
                            else:
                                lid = int(row.iloc[0,0])
                                now = datetime.now().isoformat()
                                conn.execute("UPDATE lancamentos SET anulado=1, anulado_por=?, anulado_em=?, anulacao_motivo=? WHERE id=?",
                                             (st.session_state["user"]["nome"], now, motivo_anul, lid))
                                conn.commit()
                                st.success("Lançamento anulado.")
                                log_event(conn, st.session_state["user"]["nome"], "anular_lancamento", obra_id=obra_id, casa_id=casa_id, servico_id=servico_id, detalhes={"motivo": motivo_anul, "lancamento_id": lid})
                                st.rerun()

                with cb:
                    st.markdown("### Editar estado do serviço (status/datas/executor)")
                    novo_status = st.selectbox("Novo status", ["Não iniciado","Em execução","Concluído"], index=["Não iniciado","Em execução","Concluído"].index(cur_status) if cur_status in ["Não iniciado","Em execução","Concluído"] else 0, key="cor_new_status")
                    novo_executor = st.text_input("Executor", value=cur_exec or "", key="cor_new_exec")
                    cold1, cold2 = st.columns(2)
                    nova_ini = cold1.date_input("Data de início", value=(date.fromisoformat(cur_ini) if cur_ini else date.today()), key="cor_new_ini")
                    nova_fim = cold2.date_input("Data de conclusão", value=(date.fromisoformat(cur_fim) if cur_fim else None), key="cor_new_fim")
                    motivo_estado = st.text_input("Motivo (obrigatório)", key="cor_mot_estado")

                    if st.button("Salvar estado do serviço", key="btn_save_estado"):
                        if not motivo_estado.strip():
                            st.error("Informe o motivo.")
                        else:
                            now = datetime.now().isoformat()
                            di = nova_ini.isoformat() if novo_status in ("Em execução","Concluído") else None
                            df = (nova_fim.isoformat() if novo_status=="Concluído" else None)
                            conn.execute("""INSERT INTO estado_servicos (casa_id, servico_id, status, executor, data_inicio, data_fim, updated_at)
                                            VALUES (?,?,?,?,?,?,?)
                                            ON CONFLICT(casa_id, servico_id) DO UPDATE SET
                                                status=excluded.status,
                                                executor=excluded.executor,
                                                data_inicio=excluded.data_inicio,
                                                data_fim=excluded.data_fim,
                                                updated_at=excluded.updated_at
                                         """, (casa_id, servico_id, novo_status, novo_executor or "", di, df, now))
                            conn.commit()
                            st.success("Estado do serviço atualizado.")
                            log_event(conn, st.session_state["user"]["nome"], "editar_estado_servico", obra_id=obra_id, casa_id=casa_id, servico_id=servico_id, detalhes={"motivo": motivo_estado, "novo_status": novo_status, "novo_executor": novo_executor, "data_inicio": di, "data_fim": df})
                            st.rerun()

                st.divider()
                # 7) Ação adicional: Desativar casa (se ativa)
                st.markdown("### Ativação da casa")
                ativa_info = pd.read_sql_query("SELECT ativa, ativa_em, ativa_por FROM casas WHERE id=?", conn, params=(casa_id,))
                if not ativa_info.empty and int(ativa_info.iloc[0]["ativa"]) == 1:
                    motivo_desat = st.text_input("Motivo (obrigatório) — desativar casa", key="cor_mot_desat")
                    if st.button("Desativar esta casa", key="btn_desat"):
                        if not motivo_desat.strip():
                            st.error("Informe o motivo.")
                        else:
                            conn.execute("UPDATE casas SET ativa=0, ativa_em=NULL, ativa_por=NULL WHERE id=?", (casa_id,))
                            conn.commit()
                            st.success(f"Casa {lote_sel} desativada.")
                            log_event(conn, st.session_state["user"]["nome"], "desativar_casa", obra_id=obra_id, casa_id=casa_id, detalhes={"motivo": motivo_desat, "lote": lote_sel})
                            st.rerun()
                else:
                    st.info("Casa está desativada.")


# -------- Ativar Casa --------

if page == "Ativar Casa":
    st.header("Ativar Casa (por frente de serviço)")
    obras = pd.read_sql_query("SELECT * FROM obras ORDER BY nome", conn)
    if obras.empty:
        st.warning("Não há obras cadastradas.")
    else:
        obra_nome = st.selectbox("Obra", obras["nome"].tolist())
        obra_id = int(pd.read_sql_query("SELECT id FROM obras WHERE nome=?", conn, params=(obra_nome,)).iloc[0,0])
        casas_df = pd.read_sql_query("SELECT id, lote FROM casas WHERE obra_id=? ORDER BY lote", conn, params=(obra_id,))
        lote = st.selectbox("Lote (Identificador)", casas_df["lote"].tolist())
        # [AUTO-PATCH] Seleção robusta por índice para evitar filtros vazios
        _opts = casas_df.reset_index(drop=False).rename(columns={"index":"_row_index"})
        try:
            _sel_idx = int(_opts.loc[_opts["lote"] == lote, "_row_index"].iloc[0])
            casa_row = casas_df.iloc[_sel_idx]
        except Exception:
            st.error("Não foi possível identificar a casa selecionada. Atualize a lista e tente novamente.")
            st.stop()
        casa_id = int(casa_row["id"])
        etapa = st.selectbox("Frente de serviço (etapa)", ["Reboco","Pintura","Revestimento"], index=0)

        # Estado atual da ativação dessa etapa
        ativ = pd.read_sql_query("SELECT ativa, ativa_em, ativa_por FROM casa_ativacoes WHERE casa_id=? AND etapa=?", conn, params=(casa_id, etapa))
        ativa_flag = int(ativ["ativa"].iloc[0]) if not ativ.empty else 0
        ativa_em = ativ["ativa_em"].iloc[0] if not ativ.empty else None
        ativa_por = ativ["ativa_por"].iloc[0] if not ativ.empty else None

        if ativa_flag:
            st.success(f"Casa {lote} — {etapa} já está ATIVA desde {ativa_em} por {ativa_por or '—'}.")
            if st.button("Desativar esta frente (etapa)"):
                now = datetime.now().isoformat()
                conn.execute(
                    "INSERT INTO casa_ativacoes (casa_id, etapa, ativa, ativa_em, ativa_por) VALUES (?,?,?,?,?) "
                    "ON CONFLICT(casa_id, etapa) DO UPDATE SET ativa=0, ativa_em=NULL, ativa_por=NULL",
                    (casa_id, etapa, 0, None, None)
                )
                conn.commit()
            # Seed estado_servicos a partir dos serviços da etapa/obra
            try:
                now2 = datetime.now().isoformat()
                obid = int(pd.read_sql_query("SELECT obra_id FROM casas WHERE id=?", conn, params=(casa_id,)).iloc[0,0])
                conn.execute("""
                    INSERT OR IGNORE INTO estado_servicos
                        (casa_id, servico_id, status, executor, data_inicio, data_fim, updated_at)
                    SELECT ?, s.id, 'Não iniciado', '', NULL, NULL, ?
                    FROM servicos s WHERE (s.obra_id = ? OR s.obra_id IS NULL) AND s.etapa = ?
                """, (casa_id, now2, obid, etapa))
                conn.commit()
            except Exception as _e:
                pass
        
                log_event(conn, st.session_state["user"]["nome"], "desativar_frente", obra_id=obra_id, casa_id=casa_id, detalhes={"lote": lote, "etapa": etapa})
                st.rerun()
        else:
            st.info(f"Casa {lote} — {etapa} está INATIVA.")
            if st.button("Ativar esta frente (etapa)"):
                now = datetime.now().isoformat()
                conn.execute(
                    "INSERT INTO casa_ativacoes (casa_id, etapa, ativa, ativa_em, ativa_por) VALUES (?,?,?,?,?) "
                    "ON CONFLICT(casa_id, etapa) DO UPDATE SET ativa=1, ativa_em=?, ativa_por=?",
                    (casa_id, etapa, 1, now, st.session_state["user"]["nome"], now, st.session_state["user"]["nome"])
                )
                conn.commit()
                # Semear estado_servicos para todos os serviços da ETAPA (sem previstos)
                try:
                    now2 = datetime.now().isoformat()
                    conn.execute("""
                        INSERT OR IGNORE INTO estado_servicos
                            (casa_id, servico_id, status, executor, data_inicio, data_fim, updated_at)
                        SELECT ?, s.id, 'Não iniciado', '', NULL, NULL, ?
                        FROM servicos s
                        WHERE s.etapa = ?
                    """, (casa_id, now2, etapa))
                    conn.commit()
                except Exception as _e:
                    pass
            
                log_event(conn, st.session_state["user"]["nome"], "ativar_frente", obra_id=obra_id, casa_id=casa_id, detalhes={"lote": lote, "etapa": etapa})
                st.success(f"Casa {lote} — {etapa} ativada com sucesso!")
                st.rerun()

        st.divider()
        st.subheader("Status de serviços (somente leitura)")
        # Lista serviços da etapa para visualização
        serv_df = pd.read_sql_query("SELECT id, nome FROM servicos WHERE etapa=? ORDER BY nome", conn, params=(etapa,))
        if serv_df.empty:
            st.info("Ainda não há serviços cadastrados para esta etapa.")
        else:
            estado = pd.read_sql_query("""
                SELECT s.nome as servico, COALESCE(es.status, 'Não iniciado') as status,
                       es.executor, es.data_inicio, es.data_fim
                FROM servicos s
                LEFT JOIN estado_servicos es ON es.servico_id=s.id AND es.casa_id=?
                WHERE s.etapa=?
                ORDER BY s.nome
            """, conn, params=(casa_id, etapa))
            st.dataframe(estado, use_container_width=True)

# -------- Lançamentos --------

if page == "Lançamentos" and can_view("Lançamentos"):
    st.header("Iniciar/Finalizar Serviços")
    obras = pd.read_sql_query("SELECT * FROM obras ORDER BY nome", conn)
    if obras.empty:
        st.warning("Não há obras cadastradas.")
    
    else:
        obra_nome = st.selectbox("Obra", obras["nome"].tolist())
        obra_id = int(pd.read_sql_query("SELECT id FROM obras WHERE nome=?", conn, params=(obra_nome,)).iloc[0,0])

        # Seleciona a ETAPA primeiro
        etapas_df = pd.read_sql_query("SELECT nome FROM etapas WHERE obra_id=? ORDER BY nome", conn, params=(obra_id,))
        if etapas_df.empty:
            etapas_df = pd.read_sql_query("SELECT DISTINCT etapa as nome FROM servicos WHERE (obra_id=? OR obra_id IS NULL) ORDER BY etapa", conn, params=(obra_id,))
        etapa = st.selectbox("Etapa", etapas_df["nome"].tolist(), index=0)

        # Casas ativas para a etapa (ou legado ativa=1)
        casas_df = pd.read_sql_query("""
            SELECT c.id, c.lote,
                   COALESCE(MAX(ca.ativa),0) AS ativa_etapa,
                   c.ativa AS ativa_legacy
            FROM casas c
            LEFT JOIN casa_ativacoes ca ON ca.casa_id = c.id AND ca.etapa = ?
            WHERE c.obra_id = ?
            GROUP BY c.id, c.lote, c.ativa
            ORDER BY c.lote
        """, conn, params=(etapa, obra_id))
        casas_df = casas_df[(casas_df["ativa_etapa"] == 1) | (casas_df["ativa_legacy"] == 1)]
        if casas_df.empty:
            st.info("Não há casas ativas para esta etapa nesta obra.")
            st.stop()
        lote = st.selectbox("Lote (Identificador)", casas_df["lote"].tolist())
        casa_row = casas_df[casas_df["lote"] == lote].iloc[0]
        casa_id = int(casa_row["id"])

        # Validação (mantida)
        ativ = pd.read_sql_query("SELECT ativa FROM casa_ativacoes WHERE casa_id=? AND etapa=?", conn, params=(casa_id, etapa))
        casa_legacy = int(casa_row.get("ativa_legacy", 0))

        if (ativ.empty or int(ativ.iloc[0,0]) == 0) and casa_legacy != 1:
            st.error(f"Esta casa não está ATIVA para a frente '{etapa}'. Vá na página 'Ativar Casa' e ative essa frente antes de lançar serviços.")
            st.stop()
# === MODO 1: INÍCIO MÚLTIPLO ===
        st.subheader("Iniciar Serviços")
        servs_df_all = pd.read_sql_query("SELECT id, nome FROM servicos WHERE etapa=? ORDER BY nome", conn, params=(etapa,))

        sugest_df = pd.read_sql_query("""
            SELECT s.id, s.nome, COALESCE(es.status, 'Não iniciado') AS status
            FROM servicos s
            LEFT JOIN estado_servicos es ON es.servico_id = s.id AND es.casa_id = ?
            WHERE s.etapa = ?
            ORDER BY s.nome
        """, conn, params=(casa_id, etapa))

        nao_conc = sugest_df[sugest_df["status"] != "Concluído"]
        multiselect_opts = nao_conc["nome"].tolist()

        mult_sel = st.multiselect("Selecione os serviços para INICIAR (em execução)", multiselect_opts)
        col_m1, col_m2, col_m3 = st.columns(3)
        executor_multi = col_m1.text_input("Executor (para todos os selecionados)", value="")
        data_inicio_multi = col_m2.date_input("Data de início (para todos)", value=pd.Timestamp.today().date())
        obs_multi = col_m3.text_input("Observações (opcional, para todos)", value="")

        if st.button("▶️ Iniciar serviços selecionados"):
            if not can_edit("editar_lancamentos"):
                st.error("Sem permissão para editar lançamentos.")
            elif not mult_sel:
                st.warning("Selecione pelo menos um serviço.")
            else:
                now = datetime.now().isoformat()
                id_map = dict(zip(servs_df_all["nome"].tolist(), servs_df_all["id"].tolist()))
                for nome in mult_sel:
                    sid = int(id_map[nome])
                    conn.execute("""INSERT INTO estado_servicos (casa_id, servico_id, status, executor, data_inicio, updated_at)
                                    VALUES (?,?,?,?,?,?)
                                    ON CONFLICT(casa_id, servico_id) DO UPDATE SET
                                        status=excluded.status,
                                        executor=excluded.executor,
                                        data_inicio=excluded.data_inicio,
                                        updated_at=excluded.updated_at
                                 """, (casa_id, sid, "Em execução", executor_multi or "", data_inicio_multi.isoformat(), now))
                    conn.execute("""INSERT INTO lancamentos (obra_id, casa_id, servico_id, responsavel, executor, status, data_inicio, observacoes, created_at)
                                    VALUES (?,?,?,?,?,?,?,?,?)""",

                                 (obra_id, casa_id, sid, st.session_state["user"]["nome"], executor_multi or "", "Em execução",
                                  data_inicio_multi.isoformat(), obs_multi, now))
                conn.commit()
                st.success(f"Iniciado(s): {len(mult_sel)} serviço(s).")
                log_event(conn, st.session_state["user"]["nome"], "iniciar_servicos_multiplos", obra_id=obra_id, casa_id=casa_id, detalhes={"servicos": mult_sel, "executor": executor_multi, "data_inicio": data_inicio_multi.isoformat(), "obs": obs_multi})
                st.rerun()

        st.divider()

        
# === MODO 2: FINALIZAÇÃO DE SERVIÇO ===
        st.subheader("Finalização de Serviço (opcional)")
        # Apenas serviços em execução
        serv_exec_df = pd.read_sql_query("SELECT s.id, s.nome FROM servicos s JOIN estado_servicos es ON es.servico_id=s.id AND es.casa_id=? WHERE s.etapa=? AND es.status='Em execução' ORDER BY s.nome", conn, params=(casa_id, etapa))
        if serv_exec_df.empty:
            st.info("Não há serviços em execução para finalizar nesta casa/etapa.")
        else:
            servico_nome = st.selectbox("Serviço em execução", serv_exec_df["nome"].tolist())
            servico_id = int(serv_exec_df[serv_exec_df["nome"] == servico_nome]["id"].iloc[0])
            data_fim = st.date_input("Data de conclusão", value=date.today())
            obs = st.text_area("Observações (opcional)")

            if st.button("✅ Finalizar serviço selecionado"):
                if not can_edit("editar_lancamentos"):
                    st.error("Sem permissão para editar lançamentos.")
                else:
                    now = datetime.now().isoformat()
                    conn.execute("""UPDATE estado_servicos SET status='Concluído', data_fim=?, updated_at=? WHERE casa_id=? AND servico_id=?""",
                                 (data_fim.isoformat(), now, casa_id, servico_id))
                    conn.execute("""INSERT INTO lancamentos (obra_id, casa_id, servico_id, responsavel, executor, status, data_conclusao, observacoes, created_at)
                                    VALUES (?,?,?,?,?,?,?,?,?)""",
                                 (obra_id, casa_id, servico_id, st.session_state["user"]["nome"], "", "Concluído", data_fim.isoformat(), obs, now))
                    conn.commit()
                    st.success(f"Serviço '{servico_nome}' finalizado.")
                    log_event(conn, st.session_state["user"]["nome"], "finalizar_servico", obra_id=obra_id, casa_id=casa_id, servico_id=servico_id, detalhes={"servico": servico_nome, "data_fim": data_fim.isoformat(), "obs": obs})
                    st.rerun()
        st.divider()

        st.subheader("Estado atual dos serviços desta casa/etapa")
        estado_df = pd.read_sql_query("""
            SELECT s.nome as servico, es.status, es.executor, es.data_inicio, es.data_fim, es.updated_at
            FROM servicos s
            LEFT JOIN estado_servicos es ON es.servico_id=s.id AND es.casa_id=?
            WHERE s.etapa=?
            ORDER BY s.nome
        """, conn, params=(casa_id, etapa))
        st.dataframe(estado_df, use_container_width=True)



# -------- Dashboard --------
if page == "Dashboard":
    st.header("Dashboard (visão por CASA)")

    # Filtros principais
    obras_all = pd.read_sql_query("SELECT nome FROM obras ORDER BY nome", conn)
    if obras_all.empty:
        st.info("Nenhuma obra cadastrada.")
        st.stop()
    col_f1, col_f2 = st.columns(2)
    obra_sel = col_f1.selectbox("Obra", obras_all["nome"].tolist())
    etapas_df = pd.read_sql_query("SELECT nome FROM etapas WHERE obra_id=(SELECT id FROM obras WHERE nome=?) ORDER BY nome", conn, params=(obra_sel,))
    if etapas_df.empty:
        etapas_df = pd.read_sql_query("SELECT DISTINCT etapa as nome FROM servicos WHERE (obra_id=(SELECT id FROM obras WHERE nome=?) OR obra_id IS NULL) ORDER BY etapa", conn, params=(obra_sel,))
    etapa_opts = ["Todas"] + (etapas_df["nome"].tolist() if not etapas_df.empty else [])
    etapa_sel = col_f2.selectbox("Etapa", etapa_opts, index=0)

    # Universo: casas da obra
    casas = pd.read_sql_query("""
        SELECT c.id as casa_id, c.lote, c.ativa
        FROM casas c
        JOIN obras o ON o.id = c.obra_id
        WHERE o.nome = ?
        ORDER BY c.lote
    """, conn, params=(obra_sel,))
    if casas.empty:
        st.info("Não há casas para esta obra.")
        st.stop()

    casa_ids = casas["casa_id"].tolist()


    # Ativação por frente: calcular flag de ativação por casa considerando a etapa selecionada
    if etapa_sel == "Todas":
        ativa_df = pd.read_sql_query(
            "SELECT casa_id, MAX(COALESCE(ativa,0)) AS ativa_etapa FROM casa_ativacoes GROUP BY casa_id",
            conn
        )
    else:
        ativa_df = pd.read_sql_query(
            "SELECT casa_id, MAX(COALESCE(ativa,0)) AS ativa_etapa FROM casa_ativacoes WHERE etapa = ? GROUP BY casa_id",
            conn, params=(etapa_sel,)
        )
    casas = casas.merge(ativa_df, on="casa_id", how="left")
    casas["ativa_etapa"] = casas["ativa_etapa"].fillna(0).astype(int)
    # Estado atual
    if len(casa_ids) == 0:
        st.info("Nenhuma casa selecionada.")
        st.stop()

    placeholders = ",".join(["?"] * len(casa_ids))
    est_sql = f"""
        SELECT es.casa_id, es.servico_id, es.status, es.executor, es.data_inicio, es.data_fim,
               s.etapa, s.nome as servico
        FROM estado_servicos es
        JOIN servicos s ON s.id = es.servico_id
        WHERE es.casa_id IN ({placeholders})
    """
    estado = pd.read_sql_query(est_sql, conn, params=casa_ids)
    if etapa_sel != "Todas":
        estado = estado[estado["etapa"] == etapa_sel]

    # Em execução por casa
    exec_por_casa = estado[estado["status"] == "Em execução"].groupby("casa_id")["servico_id"].nunique().rename("em_exec")
    # Totais por casa (sem previstos): usa todos os serviços da etapa selecionada
    serv_all = pd.read_sql_query("SELECT id as servico_id, etapa FROM servicos WHERE (obra_id=(SELECT id FROM obras WHERE nome=?) OR obra_id IS NULL)", conn, params=(obra_sel,))
    if etapa_sel != "Todas":
        serv_all = serv_all[serv_all["etapa"] == etapa_sel]
    total_count = int(len(serv_all))
    total_por_casa = pd.Series({cid: total_count for cid in casa_ids}, name="total_serv")

    # Concluídos por casa a partir de estado_servicos
    if not estado.empty:
        concl_por_casa = estado[estado["status"] == "Concluído"].groupby("casa_id")["servico_id"].nunique()
        concl_por_casa = concl_por_casa.rename("concluidos")
    else:
        import pandas as _pd
        concl_por_casa = _pd.Series({cid: 0 for cid in casa_ids}, name="concluidos")

    # Resumo base
    resumo = casas.merge(total_por_casa, left_on="casa_id", right_index=True, how="left")
    resumo = resumo.merge(concl_por_casa, left_on="casa_id", right_index=True, how="left")
    resumo = resumo.merge(exec_por_casa, left_on="casa_id", right_index=True, how="left")
    resumo["total_serv"] = resumo["total_serv"].fillna(0).astype(int)
    resumo["concluidos"] = resumo["concluidos"].fillna(0).astype(int)

    resumo["em_exec"] = resumo["em_exec"].fillna(0).astype(int)
    # Classificação por casa
    def classifica(row):
        if int(row.get("ativa_etapa", row.get("ativa", 0))) == 0:
            return "Não iniciado"
        if row["total_serv"] > 0 and row["concluidos"] == row["total_serv"]:
            return "Concluído"
        return "Em execução"

    resumo["status_casa"] = resumo.apply(classifica, axis=1)
    den = pd.to_numeric(resumo["total_serv"], errors="coerce").astype("Float64")
    num = pd.to_numeric(resumo["concluidos"], errors="coerce").astype("Float64")
    resumo["progresso_%"] = ((resumo["concluidos"] + resumo["em_exec"]).where(den>0, 0) / den.where(den>0, 1)) * 100
    resumo["progresso_%"] = resumo["progresso_%"].fillna(0).round(1)

    # KPIs
    c1, c2, c3 = st.columns(3)
    c1.metric("Casas — Não iniciado", int((resumo["status_casa"] == "Não iniciado").sum()))
    c2.metric("Casas — Em execução", int((resumo["status_casa"] == "Em execução").sum()))
    c3.metric("Casas — Concluídas (100%)", int((resumo["status_casa"] == "Concluído").sum()))

    st.divider()

    # Último responsável por casa (via lançamentos)
    lan_sql = f"""
        SELECT l.casa_id, l.responsavel, l.created_at, s.etapa
        FROM lancamentos l
        JOIN servicos s ON s.id = l.servico_id
        WHERE COALESCE(l.anulado,0)=0 AND l.casa_id IN ({placeholders})
    """
    lan = pd.read_sql_query(lan_sql, conn, params=casa_ids)
    if etapa_sel != "Todas":
        lan = lan[lan["etapa"] == etapa_sel]
    if not lan.empty:
        lan["created_at"] = pd.to_datetime(lan["created_at"])
        ult_resp = lan.sort_values("created_at").groupby("casa_id").tail(1)[["casa_id","responsavel"]]
        resumo = resumo.merge(ult_resp, on="casa_id", how="left")
    else:
        resumo["responsavel"] = pd.NA

    def escolhe_resp(row):
        if pd.notna(row.get("responsavel")) and str(row["responsavel"]).strip():
            return row["responsavel"]
        return ""
    resumo["responsavel_display"] = resumo.apply(escolhe_resp, axis=1)

    # Lista de casas em execução (com fallback por lançamentos se necessário)
    em_exec = resumo[resumo["status_casa"] == "Em execução"].copy()
    if em_exec.empty and not lan.empty:
        casas_lan = lan[["casa_id"]].drop_duplicates()
        em_exec = casas.merge(casas_lan, on="casa_id", how="inner")
        em_exec = em_exec.merge(resumo[["casa_id","lote","responsavel_display","progresso_%","concluidos","total_serv"]], on="casa_id", how="left")

    if em_exec.empty:
        st.info("Nenhuma casa em execução para os filtros selecionados.")
    else:
        # Garantir colunas necessárias
        if "lote" not in em_exec.columns:
            em_exec = em_exec.merge(casas[["casa_id","lote"]], on="casa_id", how="left")
        for _c in ["responsavel_display","progresso_%","concluidos","total_serv"]:
            if _c not in em_exec.columns:
                em_exec[_c] = pd.NA

        em_exec_view = em_exec[["lote","responsavel_display","progresso_%","concluidos","total_serv"]].rename(columns={"responsavel_display": "Responsável"}).sort_values(["progresso_%","lote"], ascending=[False, True])
        st.dataframe(em_exec_view, use_container_width=True)

        # Drilldown por casa
        lotes_exec = em_exec["lote"].dropna().tolist()
        if lotes_exec:
            lote_sel = st.selectbox("Selecionar casa (lote) para ver serviços", lotes_exec)
            casa_id_sel = int(em_exec.loc[em_exec["lote"] == lote_sel, "casa_id"].iloc[0])

            # Base de serviços da etapa
            serv_base = pd.read_sql_query("SELECT id as servico_id, nome as servico, etapa FROM servicos", conn)
            if etapa_sel != "Todas":
                serv_base = serv_base[serv_base["etapa"] == etapa_sel]

            det_est = estado[estado["casa_id"] == casa_id_sel].copy()
            # Garantir colunas
            for _col in ["executor","data_inicio","data_fim","status"]:
                if _col not in det_est.columns:
                    det_est[_col] = pd.NA

            det = serv_base.merge(det_est[["servico_id","status","executor","data_inicio","data_fim"]], on="servico_id", how="left")
            det["status"] = det["status"].fillna("Não iniciado")

            lan_serv = pd.read_sql_query(
                "SELECT servico_id, responsavel, status, created_at FROM lancamentos WHERE COALESCE(anulado,0)=0 AND casa_id = ?",
                conn, params=(casa_id_sel,)
            )
            if not lan_serv.empty:
                lan_serv["created_at"] = pd.to_datetime(lan_serv["created_at"])
                ini = lan_serv[lan_serv["status"] == "Em execução"].sort_values(["servico_id","created_at"]).groupby("servico_id").head(1)[["servico_id","responsavel"]].rename(columns={"responsavel":"responsavel_inicio"})
                fim = lan_serv[lan_serv["status"] == "Concluído"].sort_values(["servico_id","created_at"]).groupby("servico_id").tail(1)[["servico_id","responsavel"]].rename(columns={"responsavel":"responsavel_fim"})
                det = det.merge(ini, on="servico_id", how="left")
                det = det.merge(fim, on="servico_id", how="left")
            else:
                det["responsavel_inicio"] = pd.NA
                det["responsavel_fim"] = pd.NA

            det_view = det[["etapa","servico","status","executor","responsavel_inicio","responsavel_fim","data_inicio","data_fim"]].rename(columns={"responsavel_inicio":"Resp. início","responsavel_fim":"Resp. finalização"}).sort_values(["etapa","servico"])
            st.write(f"**Casa {lote_sel}** — Serviços (por status)")
            st.dataframe(det_view, use_container_width=True)
