import psycopg2


class Data_Base_Tools:
    def __init__(self, data_base_name, login, password, host="127.0.0.1", port="5432"):
        self.data_base_name = data_base_name
        self.login = login
        self.password = password
        self.host = host
        self.port = port

    def add_data_base_tables(self):
        conn = psycopg2.connect(database=self.data_base_name, user=self.login, password=self.password,
                                host=self.host, port=self.port)
        with conn.cursor() as cur:
            cur.execute('''CREATE TABLE IF NOT EXISTS people(
            id SERIAL primary key,
            first_name VARCHAR(60) NOT NULL,
            last_name VARCHAR(60) NOT NULL)
            ''')
            cur.execute('''create table if not exists e_mail_tab(
            id SERIAL primary key,
            id_person integer not null references people(id),
            e_mail VARCHAR(240) not null unique)''')
            cur.execute('''create table if not exists phone_number_tab(
            id SERIAL primary key,
            id_person integer not null references people(id),
            phone_number VARCHAR(15) not null unique)''')
            conn.commit()
        conn.close()

    def add_person_in_people_list(self, first_name, last_name):
        conn = psycopg2.connect(database=self.data_base_name, user=self.login, password=self.password,
                                host=self.host, port=self.port)
        with conn.cursor() as cur:
            cur.execute('insert into people(first_name, last_name)\
            values(%s, %s) RETURNING id', (first_name, last_name))
            conn.commit()
            returning = cur.fetchone()
        conn.close()
        return returning

    def add_e_mail(self, id_person, e_mail):
        conn = psycopg2.connect(database=self.data_base_name, user=self.login, password=self.password,
                                host=self.host, port=self.port)
        with conn.cursor() as cur:
            cur.execute('insert into e_mail_tab(id_person, e_mail) values(%s, %s) RETURNING id', (id_person, e_mail))
            conn.commit()
            returning = cur.fetchone()
        conn.close()
        return returning

    def add_phone_number(self, id_person, phone_number):
        conn = psycopg2.connect(database=self.data_base_name, user=self.login, password=self.password,
                                host=self.host, port=self.port)
        with conn.cursor() as cur:
            cur.execute('insert into phone_number_tab(id_person, phone_number) values(%s, %s) RETURNING id',
                        (id_person, phone_number))
            conn.commit()
            returning = cur.fetchone()
        conn.close()
        return returning

    def select_person_id(self, first_name, last_name):
        conn = psycopg2.connect(database=self.data_base_name, user=self.login, password=self.password,
                                host=self.host, port=self.port)
        with conn.cursor() as cur:
            cur.execute('select id from people where first_name=%s and last_name=%s', (first_name, last_name))
            conn.commit()
            returning = cur.fetchone()
        conn.close()
        return returning[0]

    def select_phone_number(self, id_person=None, first_name=None, last_name=None):
        conn = psycopg2.connect(database=self.data_base_name, user=self.login, password=self.password,
                                host=self.host, port=self.port)
        if id_person is None:
            with conn.cursor() as cur:
                cur.execute('select id from people where first_name=%s and last_name=%s', (first_name, last_name))
                conn.commit()
                id_person = cur.fetchone()[0]
        with conn.cursor() as cur:
            cur.execute('select phone_number from phone_number_tab where id_person = %s', (id_person,))
            conn.commit()
            returning = cur.fetchall()
        conn.close()
        return returning

    def select_e_mail(self, id_person=None, first_name=None, last_name=None):
        conn = psycopg2.connect(database=self.data_base_name, user=self.login, password=self.password,
                                host=self.host, port=self.port)
        if id_person is None:
            with conn.cursor() as cur:
                cur.execute('select id from people where first_name=%s and last_name=%s', (first_name, last_name))
                conn.commit()
                id_person = cur.fetchone()[0]
        with conn.cursor() as cur:
            cur.execute('select e_mail from e_mail_tab where id_person = %s', (id_person,))
            conn.commit()
            returning = cur.fetchall()
        conn.close()
        return returning

    def delete_phone_number(self, phone_number):
        conn = psycopg2.connect(database=self.data_base_name, user=self.login, password=self.password,
                                host=self.host, port=self.port)
        with conn.cursor() as cur:
            cur.execute('delete from phone_number_tab\
            where phone_number = %s', (phone_number,))
            conn.commit()
        conn.close()

    def delete_e_mail(self, e_mail):
        conn = psycopg2.connect(database=self.data_base_name, user=self.login, password=self.password,
                                host=self.host, port=self.port)
        with conn.cursor() as cur:
            cur.execute(f'delete from e_mail_tab\
            where e_mail = %s', (e_mail,))
            conn.commit()
        conn.close()

    def update_person_data(self, id_person=None, first_name=None, last_name=None,
                           new_first_name=None, new_last_name=None):
        conn = psycopg2.connect(database=self.data_base_name, user=self.login, password=self.password,
                                host=self.host, port=self.port)
        if new_last_name is None:
            new_last_name = last_name
        if new_first_name is None:
            new_first_name = first_name
        if id_person is None:
            with conn.cursor() as cur:
                cur.execute('select id from people where first_name=%s and last_name=%s', (first_name, last_name))
                conn.commit()
                id_person = cur.fetchone()[0]
        with conn.cursor() as cur:
            cur.execute(f'update people set first_name = %s, last_name = %s \
                    where id = %s', (new_first_name, new_last_name, id_person))
            conn.commit()
        conn.close()

    def delete_data(self, id_person=None, first_name=None, last_name=None):
        conn = psycopg2.connect(database=self.data_base_name, user=self.login, password=self.password,
                                host=self.host, port=self.port)
        if id_person is None:
            with conn.cursor() as cur:
                cur.execute('select id from people where first_name=%s and last_name=%s', (first_name, last_name))
                conn.commit()
                id_person = cur.fetchone()[0]
        with conn.cursor() as cur:
            cur.execute(f'delete from phone_number_tab\
                                where id_person = %s', (id_person,))
            cur.execute(f'delete from e_mail_tab\
                                where id_person = %s', (id_person,))
            cur.execute(f'delete from people\
                    where id_person = %s', (id_person,))
            conn.commit()
            returning = cur.fetchall()
        conn.close()
        return returning

    def select_data(self, id_person=None, first_name=None, last_name=None,
                    e_mail=None, phone_number=None):
        conn = psycopg2.connect(database=self.data_base_name, user=self.login, password=self.password,
                                host=self.host, port=self.port)
        if first_name and last_name is not None:
            with conn.cursor() as cur:
                cur.execute('select id from people where first_name=%s and last_name=%s', (first_name, last_name))
                conn.commit()
                id_person = cur.fetchone()[0]
        else:
            if e_mail is not None:
                with conn.cursor() as cur:
                    cur.execute('select id_person from e_mail_tab where e_mail = %s', (e_mail,))
                    conn.commit()
                    id_person = cur.fetchone()[0]
            else:
                if phone_number is not None:
                    with conn.cursor() as cur:
                        cur.execute('select id_person from phone_number_tab where phone_number = %s', (phone_number,))
                        conn.commit()
                        id_person = cur.fetchone()[0]
        if id_person is not None:
            with conn.cursor() as cur:
                cur.execute('select first_name, last_name from people where id = %s', (id_person,))
                person = cur.fetchall()
                cur.execute('select phone_number from phone_number_tab where id_person = %s', (id_person,))
                phone_number = cur.fetchall()
                cur.execute('select e_mail from e_mail_tab where id_person = %s', (id_person,))
                e_mail = cur.fetchall()
            conn.close()
            return person, phone_number, e_mail
        else:
            print("Ошибка ввода")


base_1 = Data_Base_Tools(data_base_name="test", login="postgres", password="9162784")
base_1.add_data_base_tables()
base_1.add_person_in_people_list('Вася', 'Пупкин')
base_1.add_phone_number(1, '+78981998')
base_1.add_e_mail(1, 'VasyaPupkin@mail.ru')
base_1.add_phone_number(1, '+88981998')
base_1.add_e_mail(1, 'VasyaPupkin_1@mail.ru')
print('base_1.select_person_id', base_1.select_person_id('Вася', 'Пупкин'))
print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
print('base_1.select_e_mail', base_1.select_e_mail(id_person=1))
print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
print('base_1.select_e_mail', base_1.select_e_mail(first_name='Вася', last_name='Пупкин'))
print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
print('base_1.select_phone_number', base_1.select_phone_number(id_person=1))
print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
print('base_1.select_phone_number', base_1.select_phone_number(first_name='Вася', last_name='Пупкин'))
print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
base_1.delete_e_mail('VasyaPupkin_1@mail.ru')
base_1.delete_phone_number('+88981998')
print('base_1.select_data(id_person=1)', base_1.select_data(id_person=1))
print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
print("base_1.select_data(first_name='Вася', last_name='Пупкин')",
      base_1.select_data(first_name='Вася', last_name='Пупкин'))
base_1.update_person_data(id_person=1, new_first_name="Петя", new_last_name="Булкин")
print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
print("base_1.select_data(e_mail='VasyaPupkin@mail.ru')", base_1.select_data(e_mail='VasyaPupkin@mail.ru'))
base_1.update_person_data(first_name="Петя", last_name="Булкин", new_first_name="Ваня", new_last_name="Рыбкин")
print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
print("base_1.select_data(phone_number='+78981998')", base_1.select_data(phone_number='+78981998'))