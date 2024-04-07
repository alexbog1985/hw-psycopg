import psycopg2


def drop_table(connection, table_name):
    with connection.cursor() as cur:
        cur.execute("""
        DROP TABLE IF EXISTS %s CASCADE;
        """ % table_name)
        connection.commit()
        print(f'Таблица {table_name} удалена')


def create_table(connection):
    with connection.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS client(
            id SERIAL PRIMARY KEY, 
            first_name VARCHAR(50) NOT NULL,
            last_name  VARCHAR(50) NOT NULL,
            email      VARCHAR(50) UNIQUE NOT NULL
        );
            
        CREATE TABLE IF NOT EXISTS phones(
            id SERIAL PRIMARY KEY,
            phone_number VARCHAR(50),
            client_id INTEGER NOT NULL
        );
        """)
        connection.commit()


def add_phone(connection, client_id, phone_number):
    with connection.cursor() as cur:
        cur.execute("""
        INSERT INTO phones(client_id, phone_number)
        VALUES (%s, %s) RETURNING phone_number, client_id;
        """, (client_id, phone_number))
        print(cur.fetchone())


def add_client(connection, first_name, last_name, email, phones=None):
    with connection.cursor() as cur:
        cur.execute("""
        INSERT INTO client(first_name, last_name, email)
        VALUES (%s, %s, %s) RETURNING id, first_name, last_name;
        """, (first_name, last_name, email))
        data_string = cur.fetchone()
        add_phone(connection, data_string[0], phones)
        print(f'Клиент {first_name} {last_name} добавлен.')


def change_client(connection, client_id, first_name=None, last_name=None, email=None, phones=None):
    with connection.cursor() as cur:
        if first_name:
            cur.execute("""
                    UPDATE client 
                       SET first_name = %s
                     WHERE id = %s;
                    """, (first_name, client_id))
            connection.commit()
        if last_name:
            cur.execute("""
                    UPDATE client 
                       SET last_name = %s
                     WHERE id = %s;
                    """, (last_name, client_id))
            connection.commit()
        if email:
            cur.execute("""
                    UPDATE client 
                       SET email = %s
                     WHERE id = %s;
                    """, (last_name, client_id))
            connection.commit()
        if phones:
            cur.execute("""
                    SELECT phone_number
                      FROM phones
                      WHERE client_id = %s;
                      """, (client_id, ))
            phones_exist = cur.fetchall()[0]
            print(phones_exist)
            for phone in phones:
                if phone not in phones_exist:
                    add_phone(connection, client_id, phone)
            connection.commit()


with psycopg2.connect(database="netology_db", user="netology", password="netology") as conn:
    drop_table(conn, "client")
    drop_table(conn, "phones")
    create_table(conn)
    add_client(conn, 'firstname', 'lastname', 'ema54il11224', '888877')
    change_client(conn, 1, 'firstnahme', 'sdffdsa', 'asdf', ['888232', '888877'])
