import sqlite3
import threading
from datetime import datetime
import uuid
from contextlib import contextmanager


# DATABASE
class Database:
    def __init__(self, db_name="bank.db"):
        self.conn = sqlite3.connect(db_name, check_same_thread=False)
        self.conn.isolation_level = None  # manual transaction control
        self.lock = threading.Lock()
        self.create_tables()

    def create_tables(self):
        with self.lock:
            cursor = self.conn.cursor()

            cursor.execute("""
            CREATE TABLE IF NOT EXISTS accounts (
                account_number TEXT PRIMARY KEY,
                customer_name TEXT,
                account_type TEXT,
                balance REAL
            )
            """)

            cursor.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                transaction_id TEXT,
                account_number TEXT,
                timestamp TEXT,
                type TEXT,
                amount REAL,
                balance_before REAL,
                balance_after REAL,
                status TEXT,
                reason TEXT
            )
            """)

            self.conn.commit()

    @contextmanager
    def transaction(self):
        cursor = self.conn.cursor()
        try:
            cursor.execute("BEGIN")
            yield cursor
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            print("DB Transaction rolled back:", e)
            raise


# TRANSACTION
class Transaction:
    def __init__(self, txn_type, amount, before, after, status, reason=""):
        self.id = str(uuid.uuid4())
        self.timestamp = datetime.now()
        self.type = txn_type
        self.amount = amount
        self.before = before
        self.after = after
        self.status = status
        self.reason = reason


#ACCOUNT
class Account:
    def __init__(self, acc_num, name, acc_type, balance):
        self.account_number = acc_num
        self.customer_name = name
        self.account_type = acc_type
        self.balance = balance
        self.lock = threading.Lock()
        self.monthly_txn_count = 0
        self.withdrawals = 0


#BANK 
class Bank:
    def __init__(self):
        self.db = Database()
        self.accounts = {}

    #  OPEN ACCOUNT
    def open_account(self, name, acc_type, deposit):
        acc_num = str(uuid.uuid4())

        if acc_type == "SAVINGS" and deposit < 100:
            print("Savings requires minimum $100")
            return None

        acc = Account(acc_num, name, acc_type, deposit)
        self.accounts[acc_num] = acc

        with self.db.lock:
            self.db.conn.execute(
                "INSERT INTO accounts VALUES (?, ?, ?, ?)",
                (acc_num, name, acc_type, deposit)
            )
            self.db.conn.commit()

        return acc_num

    # DEPOSIT
    def deposit(self, acc_num, amount):
        acc = self.accounts.get(acc_num)
        if not acc:
            return

        with acc.lock:
            with self.db.transaction() as cursor:
                before = acc.balance
                acc.balance += amount

                txn = Transaction("DEPOSIT", amount, before, acc.balance, "SUCCESS")

                cursor.execute(
                    "UPDATE accounts SET balance=? WHERE account_number=?",
                    (acc.balance, acc_num)
                )

                cursor.execute("""
                    INSERT INTO transactions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    txn.id, acc_num, str(txn.timestamp), txn.type,
                    txn.amount, txn.before, txn.after, txn.status, txn.reason
                ))

    #  WITHDRAW 
    def withdraw(self, acc_num, amount):
        acc = self.accounts.get(acc_num)
        if not acc:
            return

        with acc.lock:
            try:
                with self.db.transaction() as cursor:
                    before = acc.balance

                    # Savings rules
                    if acc.account_type == "SAVINGS":
                        if acc.balance - amount < 100:
                            raise Exception("Minimum balance violation")
                        if acc.withdrawals >= 5:
                            raise Exception("Withdrawal limit exceeded")

                    # Checking fee
                    fee = 0
                    if acc.account_type == "CHECKING":
                        if acc.monthly_txn_count >= 10:
                            fee = 2.5

                    total = amount + fee

                    if acc.balance < total:
                        raise Exception("Insufficient funds")

                    acc.balance -= total
                    acc.monthly_txn_count += 1
                    if acc.account_type == "SAVINGS":
                        acc.withdrawals += 1

                    txn = Transaction("WITHDRAW", amount, before, acc.balance, "SUCCESS")

                    cursor.execute(
                        "UPDATE accounts SET balance=? WHERE account_number=?",
                        (acc.balance, acc_num)
                    )

                    cursor.execute("""
                        INSERT INTO transactions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        txn.id, acc_num, str(txn.timestamp), txn.type,
                        txn.amount, txn.before, txn.after, txn.status, txn.reason
                    ))

            except Exception as e:
                print(f"Withdraw failed for {acc_num}:", e)

    # TRANSFER 
    def transfer(self, acc1_num, acc2_num, amount):
        acc1 = self.accounts.get(acc1_num)
        acc2 = self.accounts.get(acc2_num)

        if not acc1 or not acc2:
            return

        first, second = sorted([acc1, acc2], key=lambda x: x.account_number)

        with first.lock:
            with second.lock:
                before1 = acc1.balance
                before2 = acc2.balance

                try:
                    with self.db.transaction() as cursor:

                        if acc1.balance < amount:
                            raise Exception("Insufficient funds")

                        acc1.balance -= amount
                        acc2.balance += amount

                        cursor.execute(
                            "UPDATE accounts SET balance=? WHERE account_number=?",
                            (acc1.balance, acc1_num)
                        )
                        cursor.execute(
                            "UPDATE accounts SET balance=? WHERE account_number=?",
                            (acc2.balance, acc2_num)
                        )

                        txn1 = Transaction("TRANSFER_OUT", amount, before1, acc1.balance, "SUCCESS")
                        txn2 = Transaction("TRANSFER_IN", amount, before2, acc2.balance, "SUCCESS")

                        for txn, acc in [(txn1, acc1_num), (txn2, acc2_num)]:
                            cursor.execute("""
                                INSERT INTO transactions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """, (
                                txn.id, acc, str(txn.timestamp), txn.type,
                                txn.amount, txn.before, txn.after, txn.status, txn.reason
                            ))

                except Exception as e:
                    acc1.balance = before1
                    acc2.balance = before2
                    print("Transfer failed:", e)

    #INTEREST
    def apply_monthly_interest(self):
        for acc_num, acc in self.accounts.items():
            if acc.account_type == "SAVINGS":
                with acc.lock:
                    with self.db.transaction() as cursor:
                        before = acc.balance
                        interest = acc.balance * 0.02
                        acc.balance += interest

                        txn = Transaction("INTEREST", interest, before, acc.balance, "SUCCESS")

                        cursor.execute(
                            "UPDATE accounts SET balance=? WHERE account_number=?",
                            (acc.balance, acc_num)
                        )

                        cursor.execute("""
                            INSERT INTO transactions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            txn.id, acc_num, str(txn.timestamp), txn.type,
                            txn.amount, txn.before, txn.after, txn.status, txn.reason
                        ))

    #  STATEMENT 
    def generate_statement(self, acc_num):
        with self.db.lock:
            cursor = self.db.conn.cursor()
            cursor.execute(
                "SELECT * FROM transactions WHERE account_number=?",
                (acc_num,)
            )

            rows = cursor.fetchall()

            print(f"\nStatement for Account: {acc_num}")
            for row in rows:
                print(row)

            cursor.execute(
                "SELECT balance FROM accounts WHERE account_number=?",
                (acc_num,)
            )
            balance = cursor.fetchone()[0]
            print("Ending Balance:", balance)


# DEMO 
if __name__ == "__main__":
    bank = Bank()

    # Create 4 accounts
    a1 = bank.open_account("Riya", "CHECKING", 500)
    a2 = bank.open_account("Aman", "SAVINGS", 1000)
    a3 = bank.open_account("Neha", "CHECKING", 300)
    a4 = bank.open_account("Asmit", "SAVINGS", 200)

    # 20+ transactions (including failures)
    bank.deposit(a1, 200)
    bank.withdraw(a1, 50)
    bank.withdraw(a1, 1000)  # fail

    bank.deposit(a2, 500)
    bank.withdraw(a2, 200)
    bank.withdraw(a2, 1500)  # fail

    bank.transfer(a1, a3, 100)
    bank.transfer(a3, a2, 50)

    for _ in range(6):
        bank.withdraw(a4, 10)  # last one fails

    for _ in range(12):
        bank.deposit(a1, 10)

    bank.apply_monthly_interest()

    # Generate statements
    bank.generate_statement(a1)
    bank.generate_statement(a2)
    bank.generate_statement(a3)
    bank.generate_statement(a4)