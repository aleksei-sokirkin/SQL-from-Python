import json
import sqlalchemy as sq
from sqlalchemy.orm import sessionmaker
import os
from dotenv import load_dotenv, find_dotenv

from models import create_tables, Publisher, Book, Shop, Sale, Stock

load_dotenv(find_dotenv())

engine = sq.create_engine(os.getenv('DSN'))
create_tables(engine)
# сессия
Session = sessionmaker(bind=engine)
session = Session()


with open('tests_data.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

for record in data:
    model = {
        'publisher': Publisher,
        'shop': Shop,
        'book': Book,
        'stock': Stock,
        'sale': Sale,
    }[record.get('model')]
    session.add(model(id=record.get('pk'), **record.get('fields')))

session.commit()


def sale_list(search=input('Введите идентификатор или имя автора: ')):
    search = search
    if search.isnumeric():
        results = session.query(Book.title, Shop.name, Sale.price, Sale.date_sale) \
            .join(Publisher, Publisher.id == Book.id_publisher) \
            .join(Stock, Stock.id_book == Book.id) \
            .join(Shop, Shop.id == Stock.id_shop) \
            .join(Sale, Sale.id_stock == Stock.id) \
            .filter(Publisher.id == search).all()
        for book, shop, price, date in results:
            print(f'{book} | {shop} | {price} | {date}')
    else:
        results = session.query(Book.title, Shop.name, Sale.price, Sale.date_sale) \
            .join(Publisher, Publisher.id == Book.id_publisher) \
            .join(Stock, Stock.id_book == Book.id) \
            .join(Shop, Shop.id == Stock.id_shop) \
            .join(Sale, Sale.id_stock == Stock.id) \
            .filter(Publisher.name == search).all()
        for book, shop, price, date in results:
            print(f'{book} | {shop} | {price} | {date}')


session.close()

if __name__ == '__main__':
    sale_list()
