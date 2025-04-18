import streamlit as st
import sqlite3
import pandas as pd

# Функция подключения к базе данных
def get_connection(league_db):
    return sqlite3.connect(league_db)

# Базы данных чемпионатов
leagues = {
    "Англия Чемпионшип": "англия_чемпионшип_full_stats.db",
    "Испания Ла Лига": "испания_ла_лига_full_stats.db",
    "Испания Ла Лига 2": "испания_ла_лига_2_full_stats.db",
    "Италия Серия A": "италия_серия_a_full_stats.db",
    "Италия Серия B": "италия_серия_b_full_stats.db",
    "Португалия Примейра": "португалия_примейра_full_stats.db",
    "Россия Премьер-лига": "россия_премьер-лига_full_stats.db",
    "Франция Лига 1": "франция_лига_1_full_stats.db",
    "Франция Лига 2": "франция_лига_2_full_stats.db",
    "Шотландия Премьер-лига": "шотландия_премьер-лига_full_stats.db",
    "Венгрия Высшая лига": "венгрия_высшая_лига_full_stats.db",
    "Германия Бундеслига": "германия_бундеслига_full_stats.db",
    "Германия Бундеслига 2": "германия_бундеслига_2_full_stats.db",
    "MLS": "mls_full_stats.db",
    "Аргентина Лига Профессиональ": "аргентина_лига_профессиональ_full_stats.db",
    "Бразилия Серия А": "бразилия_серия_a_full_stats.db"
}

# Выбор чемпионата
league = st.sidebar.selectbox("🏆 Выберите чемпионат", list(leagues.keys()))

# Подключение к базе данных
conn = get_connection(leagues[league])

# Меню слева
menu = st.sidebar.radio("📋 Разделы", ["Турнирная таблица", "Матчи", "События", "Статистика команд", "Статистика игроков", "Анализ"])

if menu == "Турнирная таблица":
    st.title(f"📊 Турнирная таблица - {league}")
    query = """
    SELECT
        team,
        COUNT(fixture_id) as played,
        SUM(CASE WHEN home_away = 'home' AND goals_for > goals_against OR home_away = 'away' AND goals_for > goals_against THEN 1 ELSE 0 END) as wins,
        SUM(CASE WHEN goals_for = goals_against THEN 1 ELSE 0 END) as draws,
        SUM(CASE WHEN home_away = 'home' AND goals_for < goals_against OR home_away = 'away' AND goals_for < goals_against THEN 1 ELSE 0 END) as losses,
        SUM(goals_for) as goals_for,
        SUM(goals_against) as goals_against,
        SUM(goals_for) - SUM(goals_against) as goal_diff,
        SUM(CASE WHEN goals_for > goals_against THEN 3 WHEN goals_for = goals_against THEN 1 ELSE 0 END) as points
    FROM (
        SELECT fixture_id, home_team as team, home_goals as goals_for, away_goals as goals_against, 'home' as home_away FROM fixtures
        UNION ALL
        SELECT fixture_id, away_team as team, away_goals as goals_for, home_goals as goals_against, 'away' as home_away FROM fixtures
    ) matches
    GROUP BY team
    ORDER BY points DESC, goal_diff DESC
    """
    df_table = pd.read_sql(query, conn)
    st.dataframe(df_table)

elif menu == "Матчи":
    st.title("⚽️ Матчи")
    query = "SELECT fixture_id, date, venue, home_team, away_team, home_goals, away_goals, status FROM fixtures ORDER BY date DESC"
    df = pd.read_sql(query, conn)
    st.dataframe(df)

elif menu in ["События", "Статистика команд", "Статистика игроков"]:
    fixture_id = st.text_input("🔎 Введите ID матча", "")

    if fixture_id:
        if menu == "События":
            st.title(f"📢 События матча {fixture_id}")
            query = f"SELECT * FROM events WHERE fixture_id={fixture_id}"
            df_events = pd.read_sql(query, conn)
            st.dataframe(df_events)

        elif menu == "Статистика команд":
            st.title(f"📈 Статистика команд матча {fixture_id}")
            query = f"SELECT * FROM team_statistics WHERE fixture_id={fixture_id}"
            df_team_stats = pd.read_sql(query, conn)
            st.dataframe(df_team_stats)

        elif menu == "Статистика игроков":
            st.title(f"👤 Статистика игроков матча {fixture_id}")
            query = f"SELECT * FROM player_statistics WHERE fixture_id={fixture_id}"
            df_player_stats = pd.read_sql(query, conn)
            st.dataframe(df_player_stats)

elif menu == "Анализ":
    st.title(f"🔍 Анализ команд - {league}")

    teams = pd.read_sql("SELECT DISTINCT home_team AS team FROM fixtures UNION SELECT DISTINCT away_team FROM fixtures", conn)['team'].tolist()

    home_team = st.selectbox("🏠 Домашняя команда", sorted(teams))
    away_team = st.selectbox("🚌 Гостевая команда", sorted(teams))

    def get_avg_stats(team, n_matches=None, home_away=None):
        matches_df = pd.read_sql(f"SELECT * FROM fixtures WHERE (home_team='{team}' OR away_team='{team}') AND status='Match Finished' ORDER BY date DESC", conn)
        if home_away:
            matches_df = matches_df[matches_df[home_away] == team]
        if n_matches:
            matches_df = matches_df.head(n_matches)
        fixture_ids = matches_df['fixture_id'].tolist()
        if not fixture_ids:
            return pd.DataFrame()
        placeholders = ",".join(map(str, fixture_ids))
        query_stats = f"""
        SELECT statistic, AVG(CAST(value AS FLOAT)) as avg_value
        FROM team_statistics
        WHERE fixture_id IN ({placeholders}) AND team='{team}'
        GROUP BY statistic
        """
        return pd.read_sql(query_stats, conn).set_index('statistic')

    st.header(f"📈 Сравнение: {home_team} (Дома) vs {away_team} (Гости)")
    for label, n in [("Последние 5 матчей", 5), ("Последние 10 матчей", 10), ("Весь чемпионат", None)]:
        st.subheader(label)
        col1, col2 = st.columns(2)
        with col1:
            st.write(f"🏠 {home_team}")
            st.dataframe(get_avg_stats(home_team, n, 'home_team'))
        with col2:
            st.write(f"🚌 {away_team}")
            st.dataframe(get_avg_stats(away_team, n, 'away_team'))

conn.close()
