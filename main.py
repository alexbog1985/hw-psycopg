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
            phone_number VARCHAR(50) UNIQUE,
            client_id INTEGER NOT NULL
        );
        """)
        connection.commit()


def add_client(connection, first_name, last_name, email, phones=None):
    with connection.cursor() as cur:
        cur.execute("""
        INSERT INTO client(first_name, last_name, email)
        VALUES (%s, %s, %s) RETURNING id, first_name, last_name;
        """, (first_name, last_name, email))
        data_string = cur.fetchone()
        add_phones(connection, data_string[0], phones)
        print(f'Клиент {first_name} {last_name} добавлен.')


def find_client(connection, first_name=None, last_name=None, email=None, phone=None):
    clients_p = []
    with (connection.cursor() as cur):
        sql = """
        SELECT *
        FROM client
        WHERE first_name = %s OR last_name = %s OR email = %s;
        """
        cur.execute(sql, (str(first_name), str(last_name), str(email), ))
        clients = cur.fetchall()
        if phone is not None:
            with connection.cursor() as cur:
                sql = """
                SELECT c.id, first_name, last_name, email
                  FROM client c
                  JOIN phones p ON p.client_id = c.id
                 WHERE phone_number = %s;
                """
                cur.execute(sql, (phone, ))
                clients = cur.fetchall()
        for client in clients:
            cort = (client, [phone[1] for phone in get_phones(connection, client[0])])
            clients_p.append(cort)
    return clients_p


def add_phones(connection, client_id, phone_numbers):
    phone_exists = [p[1] for p in get_phones(connection, client_id)]
    for phone_number in phone_numbers:
        if phone_number not in phone_exists:
            with connection.cursor() as cur:
                cur.execute("""
                INSERT INTO phones(client_id, phone_number)
                VALUES (%s, %s) RETURNING phone_number, client_id;
                """, (client_id, phone_number))
                print(cur.fetchone())


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
                    """, (email, client_id))
            connection.commit()
        if phones:
            phones_exist = get_phones(connection, client_id)
            for phone in phones:
                if phone not in phones_exist:
                    add_phones(connection, client_id, [phone])


def delete_phone(connection, client_id, phone_number):
    print(get_phones(connection, client_id))
    with connection.cursor() as cur:
        sql = """
            DELETE
              FROM phones
             WHERE client_id = %s AND phone_number = %s;
        """
        cur.execute(sql, (client_id, phone_number))
        connection.commit()


def get_phones(connection, client_id):
    with connection.cursor() as cur:
        cur.execute("""
                SELECT id, phone_number
                  FROM phones
                  WHERE client_id = %s;
                  """, (client_id,))
        phones_exist = cur.fetchall()
        return phones_exist


def get_clients(connection):
    clients_p = []
    with connection.cursor() as cur:
        cur.execute("""
                SELECT * 
                  FROM client
        """)
        clients = cur.fetchall()
        for client in clients:
            cort = (client, [phone[1] for phone in get_phones(connection, client[0])])
            clients_p.append(cort)
    return clients_p


def get_client(connection, client_id):
    with connection.cursor() as cur:
        cur.execute("""
                SELECT *
                  FROM client
                 WHERE id = %s
        """, (client_id, ))
        return cur.fetchone(), [phone[1] for phone in get_phones(connection, client_id)]


def delete_client(connection, client_id):
    with connection.cursor() as cur:
        sql = """
        DELETE 
          FROM phones
         WHERE client_id = %s;
         
        DELETE
          FROM client
        WHERE id = %s;
        """
        cur.execute(sql, (client_id, client_id))
        connection.commit()


with psycopg2.connect(database="netology_db", user="netology", password="netology") as conn:
    drop_table(conn, "client")
    drop_table(conn, "phones")
    create_table(conn)
    add_client(conn, 'firstname1', 'lastname1', 'email1', ['+11111', '+11112'])
    add_client(conn, 'firstname2', 'lastname2', 'email2', ['+21111', '+21112'])
    add_client(conn, 'firstname3', 'lastname3', 'email3', ['+31111', '+31112'])
    add_client(conn, 'firstname4', 'lastname4', 'email4', ['+41111', '+41112'])
    add_client(conn, 'firstname5', 'lastname5', 'email5', ['+51111', '+51112'])
    add_client(conn, 'firstname6', 'lastname6', 'email6', ['+61111', '+61112'])
    add_client(conn, 'firstname7', 'lastname7', 'email7', ['+71111', '+71112'])
    change_client(conn, 1, 'changed1', 'changed2', 'asdf', ['888232', '888877', '+11111'])
    delete_phone(conn, 2, '+21112')
    delete_client(conn, 3)
    print(get_client(conn, 1))
    print(get_client(conn, 2))
    print(get_clients(conn))
    print(find_client(conn, 'firstname5'))
    print(find_client(conn, email='asdf', phone='+51111'))


