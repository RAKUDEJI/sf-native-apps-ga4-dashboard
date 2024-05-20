def get_complete(prompt, lang_option):
    session = get_connection()

    if lang_option == "日本語":

        system_prompt = """
            SQLの説明テキストで質問に答える


        """
    else:
        system_prompt = """
            Answer questions with an explanatory text for the SQL

        """
    try:
        response = session.sql(
            f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE('snowflake-arctic',
                ARRAY_CONSTRUCT(
                    OBJECT_CONSTRUCT('role','system','content','{system_prompt}'),
                    OBJECT_CONSTRUCT('role', 'user', 'content', '{prompt}')
                ),
                OBJECT_CONSTRUCT('temperature', 0)
            )
        """
        ).collect()[0][0]

        data = json.loads(response)
        messages_value = data["choices"][0]["messages"]

        return messages_value
    except Exception as e:
        # st.write(e)
        st.warning(get_label("key_llm_error", lang_option), icon="⚠️")
