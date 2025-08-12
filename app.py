import os
import json
import sqlite3
import pandas as pd
import streamlit as st
from datetime import datetime, date

# ======== CONFIG BÁSICA ========
APP_DIR = os.path.dirname(__file__)
DB_PATH = os.path.join(APP_DIR, "db.sqlite3")
USERS_JSON = os.path.join(APP_DIR, "usuarios.json")
UPLOAD_DIR = os.path.join(APP_DIR, "uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)

st.set_page_config(page_title="Acompanhamento de Obras", page_icon="🏗️", layout="wide")

# ======== SUPABASE (FOTOS) ========
SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip()
SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY", "").strip()
SUPABASE_BUCKET = os.getenv("SUPABASE_BUCKET", "obra-uploads")

supabase = None
if SUPABASE_URL and SUPABASE_ANON_KEY:
    try:
        from supabase import create_client
        supabase = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
    except Exception as e:
        supabase = None
        st.sidebar.warning(f"[Aviso] Supabase não inicializou: {e}")

def save_photo_and_get_url(img_bytes, obra_id, casa_id, servico_id, tag="geral"):
    """
    Salva a foto no Supabase (se configurado) e retorna URL pública.
    Caso não exista Supabase, salva localmente na pasta 'uploads' e retorna o caminho relativo.
    """
    if img_bytes is None:
        return None
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{obra_id}_{casa_id}_{servico_id}_{tag}_{ts}.jpg"

    # Se Supabase estiver OK -> Storage (público)
    if supabase:
        try:
            path_bucket = f"{obra_id}/{casa_id}/{filename}"
            supabase.storage.from_(SUPABASE_BUCKET).upload(
                path_bucket, img_bytes, file_options={"content-type": "image/jpeg", "upsert": True}
            )
            public_url = supabase.storage.from_(SUPABASE_BUCKET).get_public_url(path_bucket)
            return public_url
        except Exception as e:
            st.warning(f"Falha ao enviar foto ao Supabase, salvando localmente. Erro: {e}")

    # fallback local
    try:
        local_path = os.path.join(UPLOAD_DIR, filename)
        with open(local_path, "wb") as f:
            f.write(img_bytes)
        return f"uploads/{filename}"
    except Exception as e:
        st.error(f"Não foi possível salvar a foto: {e}")
        return None

# ======== CSS (MOBILE FRIENDLY) ========
st.markdown("""
<style>
/* inputs/botões maiores no celular */
.stButton>button { padding: 0.8rem 1rem; font-size: 1rem; }
.stTextInput>div>div>input, .stSelectbox>div>div { font-size: 1rem; }
[data-testid="stDataFrame"] { height: 60vh; }
@media (max-width: 480px) {
  .block-container { padding-top: .8rem; padding-left: .6rem; padding-right: .6rem; }
}
</style>
""", unsafe_allow_html=True)

# ======== AUTH (JSON) ========
def load_users():
    try:
        with open(USERS_JSON, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"users": []}

def save_users(data):
    with open(USERS_JSON, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def require_login():
    return "user" in st.session_state and st.session_state["user"]

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
    if hasattr(user_dict, "to_dict"):
        user_dict = user_dict.to_dict()
    role = user_dict.get("role", "user") if isinstance(user_dict, dict) else "user"
    p = dict(_default_permissoes(role))
    up_raw = (user_dict.get("permissoes") if isinstance(user_dict, dict) else None)
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

# ======== DB (SQLite AGORA) ========
def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def init_db(conn):
    cur = conn.cursor()
    cur.execute("PRAGMA foreign_keys = ON;")
    cur.execute("CREATE TABLE IF NOT EXISTS obras (id INTEGER PRIMARY KEY AUTOINCREMENT, nome TEXT UNIQUE NOT NULL)")
    cur.execute("""CREATE TABLE IF NOT EXISTS etapas (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        obra_id INTEGER NOT NULL,
        nome TEXT NOT NULL,
        UNIQUE (obra_id, nome),
        FOREIGN KEY (obra_id) REFERENCES obras(id)
    )""")
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
    cur.execute("CREATE TABLE IF NOT EXISTS servicos (id INTEGER PRIMARY KEY AUTOINCREMENT, nome TEXT NOT NULL, etapa TEXT NOT NULL, obra_id INTEGER, UNIQUE(nome, etapa))")
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
        anulado INTEGER NOT NULL DEFAULT 0,
        anulado_por TEXT,
        anulado_em TEXT,
        anulacao_motivo TEXT,
        FOREIGN KEY (obra_id) REFERENCES obras(id) ON DELETE CASCADE,
        FOREIGN KEY (casa_id) REFERENCES casas(id) ON DELETE CASCADE,
        FOREIGN KEY (servico_id) REFERENCES servicos(id) ON DELETE CASCADE
    )""")
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

def log_event(conn, usuario, acao, obra_id=None, casa_id=None, servico_id=None, detalhes=None):
    ts = datetime.now().isoformat()
    det = json.dumps(detalhes, ensure_ascii=False) if detalhes is not None else None
    conn.execute(
        "INSERT INTO auditoria (timestamp, usuario, acao, obra_id, casa_id, servico_id, detalhes) VALUES (?,?,?,?,?,?,?)",
        (ts, usuario, acao, obra_id, casa_id, servico_id, det)
    )
    conn.commit()

# ======== INICIALIZAÇÃO ========
conn = get_conn()
init_db(conn)

# ======== LOGIN ========
st.sidebar.title("🏗️ Acompanhamento de Obras — Login")
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

# ======== PÁGINAS ========

# Minha Conta
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
                for u in users:
                    if u.get("username") == uname and u.get("ativo", True):
                        if u.get("password") != current_pw:
                            st.error("Senha atual incorreta.")
                            break
                        u["password"] = new_pw
                        save_users({"users": users})
                        st.success("Senha alterada com sucesso.")
                        log_event(conn, user["nome"], "alterar_senha", detalhes={"username": uname})
                        break

# Base de Dados (obras/etapas/serviços) – idêntico ao seu fluxo
if page == "Base de Dados" and can_view("Base de Dados"):
    st.header("Base de Dados")
    tabs = st.tabs(["Obras", "Etapas", "Base de Dados"])

    # Obras
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

    # Etapas
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

    # Serviços
    with tabs[2]:
        st.subheader("Serviços por Etapa")
        obras_df = pd.read_sql_query("SELECT id, nome FROM obras ORDER BY nome", conn)
        if obras_df.empty:
            st.info("Crie uma obra primeiro.")
        else:
            obra_sel = st.selectbox("Obra", obras_df["nome"].tolist(), key="bd_sv_ob")
            obra_id = int(obras_df.loc[obras_df["nome"] == obra_sel, "id"].iloc[0])
            etapas_df = pd.read_sql_query("SELECT id, nome FROM etapas WHERE obra_id=? ORDER BY nome", conn, params=(obra_id,))
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

# Logs
if page == "Logs" and can_view("Logs"):
    st.header("Logs do Sistema")
    col1, col2, col3 = st.columns(3)
    usuarios = pd.read_sql_query("SELECT DISTINCT COALESCE(usuario,'') as usuario FROM auditoria ORDER BY usuario", conn)
    usuario_sel = col1.selectbox("Usuário", ["Todos"] + [u for u in usuarios["usuario"].tolist() if u])
    acoes = pd.read_sql_query("SELECT DISTINCT acao FROM auditoria ORDER BY acao", conn)
    acao_sel = col2.selectbox("Ação", ["Todas"] + acoes["acao"].dropna().tolist())
    limite = col3.number_input("Mostrar últimos (registros)", min_value=50, max_value=100000, value=200, step=50)

    base_sql = "SELECT timestamp, usuario, acao, obra_id, casa_id, servico_id, detalhes FROM auditoria"
    where, params = [], []
    if usuario_sel != "Todos":
        where.append("usuario = ?"); params.append(usuario_sel)
    if acao_sel != "Todas":
        where.append("acao = ?"); params.append(acao_sel)
    if where: base_sql += " WHERE " + " AND ".join(where)
    base_sql += " ORDER BY id DESC LIMIT ?"; params.append(int(limite))

    df_logs = pd.read_sql_query(base_sql, conn, params=params)
    st.dataframe(df_logs, use_container_width=True)

# Admin (usuários) – igual ao seu, mantido
if page == "Admin" and can_view("Admin") and can_edit("editar_usuarios"):
    st.header("Administração de Usuários")
    data = load_users()
    if len(data.get("users", [])) == 0:
        st.info("Nenhum usuário no arquivo usuarios.json")
    else:
        df_users = pd.DataFrame(data["users"])
        def _perm_resumo(u):
            p = _merge_permissoes(u.to_dict() if hasattr(u, "to_dict") else u)
            chaves = ["ver_ativar_casa","ver_lancamentos","ver_dashboard","ver_servicos","ver_logs","ver_admin",
                      "editar_lancamentos","editar_servicos","editar_usuarios"]
            return ", ".join([k for k in chaves if p.get(k)])
        df_users["permissoes_resumo"] = df_users.apply(_perm_resumo, axis=1)
        cols_show = [c for c in ["username","nome","role","ativo","permissoes_resumo"] if c in df_users.columns]
        st.dataframe(df_users[cols_show], use_container_width=True)

# Ativar Casa – igual ao seu fluxo (sem mudanças funcionais)
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
        casa_row = casas_df[casas_df["lote"] == lote].iloc[0]
        casa_id = int(casa_row["id"])
        etapa = st.selectbox("Frente de serviço (etapa)", ["Reboco","Pintura","Revestimento"], index=0)

        ativ = pd.read_sql_query("SELECT ativa, ativa_em, ativa_por FROM casa_ativacoes WHERE casa_id=? AND etapa=?", conn, params=(casa_id, etapa))
        ativa_flag = int(ativ["ativa"].iloc[0]) if not ativ.empty else 0
        ativa_em = ativ["ativa_em"].iloc[0] if not ativ.empty else None
        ativa_por = ativ["ativa_por"].iloc[0] if not ativ.empty else None

        if ativa_flag:
            st.success(f"Casa {lote} — {etapa} já está ATIVA desde {ativa_em} por {ativa_por or '—'}.")
            if st.button("Desativar esta frente (etapa)"):
                conn.execute(
                    "INSERT INTO casa_ativacoes (casa_id, etapa, ativa, ativa_em, ativa_por) VALUES (?,?,?,?,?) "
                    "ON CONFLICT(casa_id, etapa) DO UPDATE SET ativa=0, ativa_em=NULL, ativa_por=NULL",
                    (casa_id, etapa, 0, None, None)
                )
                conn.commit()
        else:
            st.info(f"Casa {lote} — {etapa} está INATIVA.")
            if st.button("Ativar esta frente (etapa)"):
                now = datetime.now().isoformat()
                conn.execute(
                    "INSERT INTO casa_ativacoes (casa_id, etapa, ativa, ativa_em, ativa_por) VALUES (?,?,?,?,?) "
                    "ON CONFLICT(casa_id, etapa) DO UPDATE SET ativa=1, ativa_em=?, ativa_por=?",
                    (casa_id, etapa, 1, now, st.session_state['user']['nome'])
                )
                conn.commit()
                st.success(f"Casa {lote} — {etapa} ativada com sucesso!")

# Lançamentos – câmera opcional no início e na finalização
if page == "Lançamentos" and can_view("Lançamentos"):
    st.header("Iniciar/Finalizar Serviços")
    obras = pd.read_sql_query("SELECT * FROM obras ORDER BY nome", conn)
    if obras.empty:
        st.warning("Não há obras cadastradas.")
    else:
        obra_nome = st.selectbox("Obra", obras["nome"].tolist())
        obra_id = int(pd.read_sql_query("SELECT id FROM obras WHERE nome=?", conn, params=(obra_nome,)).iloc[0,0])

        etapas_df = pd.read_sql_query("SELECT nome FROM etapas WHERE obra_id=? ORDER BY nome", conn, params=(obra_id,))
        if etapas_df.empty:
            etapas_df = pd.read_sql_query("SELECT DISTINCT etapa as nome FROM servicos WHERE (obra_id=? OR obra_id IS NULL) ORDER BY etapa", conn, params=(obra_id,))
        etapa = st.selectbox("Etapa", etapas_df["nome"].tolist(), index=0)

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

        # ===== INÍCIO MÚLTIPLO =====
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
        mult_sel = st.multiselect("Selecione os serviços para INICIAR (em execução)", nao_conc["nome"].tolist())
        col_m1, col_m2, col_m3 = st.columns(3)
        executor_multi = col_m1.text_input("Executor (para todos)", value="")
        data_inicio_multi = col_m2.date_input("Data de início (para todos)", value=pd.Timestamp.today().date())
        obs_multi = col_m3.text_input("Observações (opcional)", value="")
        add_photo_ini = st.checkbox("Anexar foto agora (opcional)", value=False)
        foto_ini = st.camera_input("Foto (início — opcional)") if add_photo_ini else None

        if st.button("▶️ Iniciar serviços selecionados"):
            if not can_edit("editar_lancamentos"):
                st.error("Sem permissão para editar lançamentos.")
            elif not mult_sel:
                st.warning("Selecione pelo menos um serviço.")
            else:
                now = datetime.now().isoformat()
                id_map = dict(zip(servs_df_all["nome"].tolist(), servs_df_all["id"].tolist()))
                foto_url = None
                if foto_ini is not None:
                    foto_url = save_photo_and_get_url(foto_ini.getvalue(), obra_id, casa_id, 0, tag="inicio")
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
                    conn.execute("""INSERT INTO lancamentos (obra_id, casa_id, servico_id, responsavel, executor, status, data_inicio, observacoes, foto_path, created_at)
                                    VALUES (?,?,?,?,?,?,?,?,?,?)""",
                                 (obra_id, casa_id, sid, st.session_state["user"]["nome"], executor_multi or "", "Em execução",
                                  data_inicio_multi.isoformat(), obs_multi, foto_url, now))
                conn.commit()
                st.success(f"Iniciado(s): {len(mult_sel)} serviço(s).")
                st.rerun()

        st.divider()

        # ===== FINALIZAÇÃO =====
        st.subheader("Finalização de Serviço (opcional)")
        serv_exec_df = pd.read_sql_query("""
            SELECT s.id, s.nome
            FROM servicos s
            JOIN estado_servicos es ON es.servico_id=s.id AND es.casa_id=?
            WHERE s.etapa=? AND es.status='Em execução'
            ORDER BY s.nome
        """, conn, params=(casa_id, etapa))
        if serv_exec_df.empty:
            st.info("Não há serviços em execução para finalizar nesta casa/etapa.")
        else:
            servico_nome = st.selectbox("Serviço em execução", serv_exec_df["nome"].tolist())
            servico_id = int(serv_exec_df[serv_exec_df["nome"] == servico_nome]["id"].iloc[0])
            data_fim = st.date_input("Data de conclusão", value=date.today())
            obs = st.text_area("Observações (opcional)")
            add_photo_fim = st.checkbox("Anexar foto na finalização (opcional)", value=False)
            foto_fim = st.camera_input("Foto (finalização — opcional)") if add_photo_fim else None

            if st.button("✅ Finalizar serviço selecionado"):
                if not can_edit("editar_lancamentos"):
                    st.error("Sem permissão para editar lançamentos.")
                else:
                    now = datetime.now().isoformat()
                    foto_url = None
                    if foto_fim is not None:
                        foto_url = save_photo_and_get_url(foto_fim.getvalue(), obra_id, casa_id, servico_id, tag="final")
                    conn.execute("""UPDATE estado_servicos
                                    SET status='Concluído', data_fim=?, updated_at=?
                                    WHERE casa_id=? AND servico_id=?""",
                                 (data_fim.isoformat(), now, casa_id, servico_id))
                    conn.execute("""INSERT INTO lancamentos (obra_id, casa_id, servico_id, responsavel, executor, status, data_conclusao, observacoes, foto_path, created_at)
                                    VALUES (?,?,?,?,?,?,?,?,?,?)""",
                                 (obra_id, casa_id, servico_id, st.session_state["user"]["nome"], "", "Concluído",
                                  data_fim.isoformat(), obs, foto_url, now))
                    conn.commit()
                    st.success(f"Serviço '{servico_nome}' finalizado.")
                    st.rerun()

# Dashboard – mantido (sem alterações relevantes ao pedido)
if page == "Dashboard":
    st.header("Dashboard (visão por CASA)")
    obras_all = pd.read_sql_query("SELECT nome FROM obras ORDER BY nome", conn)
    if obras_all.empty:
        st.info("Nenhuma obra cadastrada."); st.stop()
    col_f1, col_f2 = st.columns(2)
    obra_sel = col_f1.selectbox("Obra", obras_all["nome"].tolist())
    etapas_df = pd.read_sql_query("SELECT nome FROM etapas WHERE obra_id=(SELECT id FROM obras WHERE nome=?) ORDER BY nome", conn, params=(obra_sel,))
    if etapas_df.empty:
        etapas_df = pd.read_sql_query("SELECT DISTINCT etapa as nome FROM servicos WHERE (obra_id=(SELECT id FROM obras WHERE nome=?) OR obra_id IS NULL) ORDER BY etapa", conn, params=(obra_sel,))
    etapa_opts = ["Todas"] + (etapas_df["nome"].tolist() if not etapas_df.empty else [])
    etapa_sel = col_f2.selectbox("Etapa", etapa_opts, index=0)

    casas = pd.read_sql_query("""
        SELECT c.id as casa_id, c.lote, c.ativa
        FROM casas c
        JOIN obras o ON o.id = c.obra_id
        WHERE o.nome = ?
        ORDER BY c.lote
    """, conn, params=(obra_sel,))
    if casas.empty:
        st.info("Não há casas para esta obra."); st.stop()
    casa_ids = casas["casa_id"].tolist()

    if etapa_sel == "Todas":
        ativa_df = pd.read_sql_query("SELECT casa_id, MAX(COALESCE(ativa,0)) AS ativa_etapa FROM casa_ativacoes GROUP BY casa_id", conn)
    else:
        ativa_df = pd.read_sql_query("SELECT casa_id, MAX(COALESCE(ativa,0)) AS ativa_etapa FROM casa_ativacoes WHERE etapa = ? GROUP BY casa_id", conn, params=(etapa_sel,))
    casas = casas.merge(ativa_df, on="casa_id", how="left").fillna({"ativa_etapa":0})
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

    exec_por_casa = estado[estado["status"] == "Em execução"].groupby("casa_id")["servico_id"].nunique().rename("em_exec")
    serv_all = pd.read_sql_query("SELECT id as servico_id, etapa FROM servicos WHERE (obra_id=(SELECT id FROM obras WHERE nome=?) OR obra_id IS NULL)", conn, params=(obra_sel,))
    if etapa_sel != "Todas":
        serv_all = serv_all[serv_all["etapa"] == etapa_sel]
    total_count = int(len(serv_all))
    total_por_casa = pd.Series({cid: total_count for cid in casa_ids}, name="total_serv")
    concl_por_casa = estado[estado["status"] == "Concluído"].groupby("casa_id")["servico_id"].nunique().rename("concluidos") if not estado.empty else pd.Series({cid:0 for cid in casa_ids}, name="concluidos")

    resumo = casas.merge(total_por_casa, left_on="casa_id", right_index=True, how="left") \
                  .merge(concl_por_casa, left_on="casa_id", right_index=True, how="left") \
                  .merge(exec_por_casa, left_on="casa_id", right_index=True, how="left")
    resumo = resumo.fillna({"total_serv":0, "concluidos":0, "em_exec":0}).astype({"total_serv":int, "concluidos":int, "em_exec":int})
    def classifica(row):
        if int(row.get("ativa_etapa", row.get("ativa", 0))) == 0:
            return "Não iniciado"
        if row["total_serv"] > 0 and row["concluidos"] == row["total_serv"]:
            return "Concluído"
        return "Em execução"
    resumo["status_casa"] = resumo.apply(classifica, axis=1)
    st.metric("Casas — Não iniciado", int((resumo["status_casa"] == "Não iniciado").sum()))
    st.metric("Casas — Em execução", int((resumo["status_casa"] == "Em execução").sum()))
    st.metric("Casas — Concluídas (100%)", int((resumo["status_casa"] == "Concluído").sum()))
