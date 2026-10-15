"""Run with: python examples/producer.py application.db order-42"""

import argparse

from relayledger import connect, enqueue, initialize, transaction


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('database')
    parser.add_argument('order_id')
    args = parser.parse_args()
    database = connect(args.database,create=True)
    try:
        initialize(database)
        with transaction(database):
            database.execute('CREATE TABLE IF NOT EXISTS orders(id TEXT PRIMARY KEY,state TEXT NOT NULL)')
            # This application command can be retried without creating another event.
            database.execute("INSERT INTO orders VALUES(?,'confirmed') ON CONFLICT(id) DO NOTHING",(args.order_id,))
            event_id = enqueue(database,key=args.order_id+':confirmed:v1',topic='order.confirmed',
                               stream=args.order_id,data={'order_id':args.order_id,'state':'confirmed'})
        print(event_id)
    finally:
        database.close()


if __name__=='__main__':
    main()
