from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.decorators import dag, task
import datetime
import json
import os


S3_BUCKET = "dewi-etl-data-dumps"
LEDGER_DUMP_CHUNK_SIZE_BLOCKS = int(25e3)


def get_data_from_postgres(query: str, conn_id="POSTGRES_DEFAULT"):
    hook = PostgresHook(postgres_conn_id=conn_id)
    conn = hook.get_conn()
    cur = conn.cursor()
    cur.execute(query)
    return cur.fetchall()


def get_current_height() -> int:
    res = get_data_from_postgres("""SELECT max(height) from blocks;""")
    return res[0][0]


def convert_result_to_csv(res, columns_list, output_path):
    import pandas as pd
    df = pd.DataFrame(res)
    df.columns = columns_list
    df.to_csv(output_path)


def upload_to_s3_operator(task_id: str, filename: str, dest_key: str, compressed=True):
    return LocalFilesystemToS3Operator(task_id=task_id,
                                       aws_conn_id="AWS_DEFAULT",
                                       filename=filename,
                                       dest_key=dest_key,
                                       dest_bucket=S3_BUCKET,
                                       replace=True,
                                       gzip=compressed)


def download_from_s3_hook(key: str):
    hook = S3Hook(aws_conn_id="AWS_DEFAULT")
    if hook.check_for_key(key, bucket_name=S3_BUCKET):
        return hook.download_file(key=key, bucket_name=S3_BUCKET)
    else:
        return False


def upload_to_s3_hook(filename: str, key: str, compressed=True):
    hook = S3Hook(aws_conn_id="AWS_DEFAULT")
    hook.load_file(filename=filename,
                   key=key,
                   bucket_name=S3_BUCKET,
                   replace=True,
                   gzip=compressed)



def get_ledger_dump_status():
    status_filename = download_from_s3_hook("status.json")
    if status_filename is False:
        return False
    else:
        with open(status_filename, "r") as f:
            status_dict = json.load(f)
        return status_dict


def garbage_collection(files_to_delete: list):
    for file in files_to_delete:
        os.remove(file)


@dag(
    dag_id="etl_inventory_dumps",
    start_date=datetime.datetime(2022, 2, 10),
    schedule_interval="0 7 * * *",
    catchup=False)
def etl_inventory_dumps():
    """
    ### Dump current blockchain-etl inventories
    """
    @task(task_id="get_gateway_inventory")
    def get_gateway_inventory(current_height):
        query = """SELECT address, owner, location, last_poc_challenge, last_poc_onion_key_hash, first_block, last_block, nonce,
                            name, first_timestamp, reward_scale, elevation, gain, location_hex, mode, payer
                   FROM gateway_inventory;"""

        res = get_data_from_postgres(query)
        columns_list = ["address", "owner", "location", "last_poc_challenge", "last_poc_onion_key_hash", "first_block",
                      "last_block", "nonce", "name", "first_timestamp", "reward_scale", "elevation", "gain", "location_hex", "mode", "payer"]
        output_path = f"gateway_inventory_{str(current_height).zfill(8)}.csv"
        convert_result_to_csv(res, columns_list, output_path)
        return output_path

    @task(task_id="get_account_inventory")
    def get_account_inventory(current_height):
        query = """SELECT address, balance, nonce, dc_balance, dc_nonce, security_balance, first_block, last_block, staked_balance
                       FROM account_inventory;"""

        res = get_data_from_postgres(query)
        columns_list = ["address", "balance", "nonce", "dc_balance", "dc_nonce", "security_balance", "first_block", "last_block", "staked_balance"]
        output_path = f"account_inventory_{str(current_height).zfill(8)}.csv"
        convert_result_to_csv(res, columns_list, output_path)
        return output_path

    @task(task_id="get_gateway_status")
    def get_gateway_status(current_height):
        query = """SELECT address, online, block, updated_at, poc_interval, last_challenge, peer_timestamp, listen_addrs
                           FROM gateway_status;"""

        res = get_data_from_postgres(query)
        columns_list = ["address", "online", "block", "updated_at", "poc_interval", "last_challenge", "peer_timestamp", "listen_addrs"]
        output_path = f"gateway_status_{str(current_height).zfill(8)}.csv"
        convert_result_to_csv(res, columns_list, output_path)
        return output_path

    @task(task_id="get_locations")
    def get_locations(current_height):
        query = """SELECT location, long_street, short_street, long_city, short_city, long_state, short_state, long_country, short_country,
                        search_city, city_id, geometry
                               FROM locations;"""

        res = get_data_from_postgres(query)
        columns_list = ["location", "long_street", "short_street", "long_city", "short_city", "long_state", "short_state", "long_country", "short_country",
                        "search_city", "city_id", "geometry"]
        output_path = f"locations_{str(current_height).zfill(8)}.csv"
        convert_result_to_csv(res, columns_list, output_path)
        return output_path

    @task(task_id="garbage_collection")
    def garbage_collection_task(files_to_delete, with_gz=True):
        garbage_collection(files_to_delete)
        if with_gz:
            garbage_collection_task([f"{file}.gz" for file in files_to_delete])

    current_height = get_current_height()

    locations_path = get_locations(current_height)
    upload_locations = upload_to_s3_operator(task_id="upload_locations_to_s3",
                                            filename=locations_path,
                                            dest_key=f"locations/{locations_path}.gz")

    gateway_inventory_path = get_gateway_inventory(current_height)

    upload_gateway_inventory = upload_to_s3_operator(task_id="upload_gateway_inventory_to_s3",
                                            filename=gateway_inventory_path,
                                            dest_key=f"gateway_inventory/{gateway_inventory_path}.gz")
    upload_gateway_inventory.doc_md = """#### Upload Current Gateway Inventory to S3 Bucket"""

    account_inventory_path = get_account_inventory(current_height)

    upload_account_inventory = upload_to_s3_operator(task_id="upload_account_inventory_to_s3",
                                            filename=account_inventory_path,
                                            dest_key=f"account_inventory/{account_inventory_path}.gz")
    upload_account_inventory.doc_md = """#### Upload Current Account Inventory to S3 Bucket"""

    gateway_status_path = get_gateway_status(current_height)

    upload_gateway_status = upload_to_s3_operator(task_id="upload_gateway_status_to_s3",
                                                     filename=gateway_status_path,
                                                     dest_key=f"gateway_status/{gateway_status_path}.gz")
    upload_gateway_status.doc_md = """#### Upload Current Gateway Status to S3 Bucket"""

    files_to_delete = [gateway_inventory_path, account_inventory_path, gateway_status_path, locations_path]
    [upload_locations, upload_gateway_inventory, upload_account_inventory, upload_gateway_status] >> garbage_collection_task(files_to_delete)

@dag(
    dag_id="etl_ledger_dumps",
    start_date=datetime.datetime(2022, 2, 10),
    schedule_interval="@once",
    catchup=False)
def etl_ledger_dumps():
    """
    ### Dump historical blockchain-etl ledger entries
    """
    current_height = get_current_height()

    status_dict = get_ledger_dump_status()
    if status_dict is False:
        status_dict = {
            "blocks": 1.2e6,
            "challenge_receipts_parsed": 0,
            "transactions_payment_v1": 1.2e6,
            "transactions_payment_v2": 1.2e6,
            "rewards": 0.725e6,
            "packets": 1.2e6
        }


    @task(task_id="dump_challenge_receipts_parsed")
    def dump_challenge_receipts_parsed(current_height: int, dump_height: int):
        block_min = int(dump_height)
        block_max = int(block_min + LEDGER_DUMP_CHUNK_SIZE_BLOCKS)
        while (current_height - block_min) > LEDGER_DUMP_CHUNK_SIZE_BLOCKS:
            try:
                res = get_data_from_postgres(f"""SELECT 
                            block,
                            hash,
                            time,
                            transmitter_name,
                            transmitter_address,
                            origin,
                            witness_owner,
                            witness_name,
                            witness_address,
                            witness_is_valid,
                            witness_invalid_reason,
                            witness_signal,
                            witness_snr,
                            witness_channel,
                            witness_datarate,
                            witness_frequency,
                            witness_location,
                            witness_timestamp
                    FROM challenge_receipts_parsed
                    WHERE block >= {block_min} AND block < {block_max};""")
                columns_list = ["block", "hash", "time", "transmitter_name", "transmitter_address", "origin", "witness_owner", "witness_name",
                                "witness_address", "witness_is_valid", "witness_invalid_reason", "witness_signal", "witness_snr", "witness_channel",
                                "witness_datarate", "witness_frequency", "witness_location", "witness_timestamp"]
                output_path = f"challenge_receipts_parsed_{str(block_min).zfill(8)}-{str(block_max - 1).zfill(8)}.csv"
                convert_result_to_csv(res, columns_list, output_path)
                upload_to_s3_hook(output_path, f"challenge_receipts_parsed/{output_path}.gz")
                os.remove(output_path)
                os.remove(f"{output_path}.gz")

            except ValueError:
                # no results for query
                pass
            block_min = block_max
            block_max += LEDGER_DUMP_CHUNK_SIZE_BLOCKS
            print(f"{block_min} to {block_max}")
        dump_height = block_min
        return int(dump_height)


    @task(task_id="dump_rewards")
    def dump_rewards(current_height: int, dump_height: int):
        block_min = int(dump_height)
        block_max = int(block_min + LEDGER_DUMP_CHUNK_SIZE_BLOCKS)
        while (current_height - block_min) > LEDGER_DUMP_CHUNK_SIZE_BLOCKS:
            try:
                res = get_data_from_postgres(f"""SELECT 
                            block,
                            transaction_hash,
                            time,
                            account,
                            gateway,
                            amount                        
                    FROM rewards 
                    WHERE block >= {block_min} AND block < {block_max};""")
                columns_list = ["block", "transaction_hash", "time", "account", "gateway", "amount"]
                output_path = f"rewards_{str(block_min).zfill(8)}-{str(block_max - 1).zfill(8)}.csv"
                convert_result_to_csv(res, columns_list, output_path)
                upload_to_s3_hook(output_path, f"rewards/{output_path}.gz")
                os.remove(output_path)
                os.remove(f"{output_path}.gz")

            except ValueError:
                # no results for query
                pass
            print(f"{block_min} to {block_max}")
            block_min = block_max
            block_max += LEDGER_DUMP_CHUNK_SIZE_BLOCKS
        dump_height = block_min
        return int(dump_height)


    @task(task_id="dump_packets")
    def dump_packets(current_height: int, dump_height: int):
        block_min = int(dump_height)
        block_max = int(block_min + LEDGER_DUMP_CHUNK_SIZE_BLOCKS)
        while (current_height - block_min) > LEDGER_DUMP_CHUNK_SIZE_BLOCKS:
            try:
                res = get_data_from_postgres(f"""SELECT 
                                block,
                                transaction_hash,
                                time,
                                gateway,
                                num_packets,
                                num_dcs
                        FROM packets 
                        WHERE block >= {block_min} AND block < {block_max};""")
                columns_list = ["block", "transaction_hash", "time", "gateway", "num_packets", "num_dcs"]
                output_path = f"packets_{str(block_min).zfill(8)}-{str(block_max - 1).zfill(8)}.csv"
                convert_result_to_csv(res, columns_list, output_path)
                upload_to_s3_hook(output_path, f"packets/{output_path}.gz", compressed=True)
                os.remove(output_path)
                os.remove(f"{output_path}.gz")

            except ValueError:
                # no results for query
                pass
            print(f"{block_min} to {block_max}")
            block_min = block_max
            block_max += LEDGER_DUMP_CHUNK_SIZE_BLOCKS
        dump_height = block_min
        return int(dump_height)


    @task(task_id="dump_transactions_payment_v1")
    def dump_transactions_payment_v1(current_height: int, dump_height: int):
        block_min = int(dump_height)
        block_max = int(block_min + LEDGER_DUMP_CHUNK_SIZE_BLOCKS)
        while (current_height - block_min) > LEDGER_DUMP_CHUNK_SIZE_BLOCKS:
            try:
                res = get_data_from_postgres(f"""SELECT 
                        block
                        -- , hash
                        -- , type
                        , fields::json->>'fee' as fee
                        , fields::json->>'hash' as hash
                        , fields::json->>'type' as type
                        , fields::json->>'nonce' as nonce
                        , fields::json->>'payee' as payee
                        , fields::json->>'payer' as payer
                        , (fields::json->>'amount')::bigint as amount
                FROM transactions 
                WHERE type = 'payment_v1' AND block >= {block_min} AND block < {block_max};""")
                columns_list = ["block", "fee", "hash", "type", "nonce", "payee", "payer", "amount"]
                output_path = f"transactions_payment_v1_{str(block_min).zfill(8)}-{str(block_max-1).zfill(8)}.csv"
                convert_result_to_csv(res, columns_list, output_path)
                upload_to_s3_hook(output_path, f"transactions_payment_v1/{output_path}.gz")
                os.remove(output_path)
                os.remove(f"{output_path}.gz")

            except ValueError:
                # no results for query
                pass
            print(f"{block_min} to {block_max}")
            block_min = block_max
            block_max += LEDGER_DUMP_CHUNK_SIZE_BLOCKS
        dump_height = block_min
        return int(dump_height)


    @task(task_id="dump_transactions_payment_v2")
    def dump_transactions_payment_v2(current_height: int, dump_height: int):
        block_min = int(dump_height)
        block_max = int(block_min + LEDGER_DUMP_CHUNK_SIZE_BLOCKS)
        while (current_height - block_min) > LEDGER_DUMP_CHUNK_SIZE_BLOCKS:
            try:
                res = get_data_from_postgres(f"""with payment_v2_parsed as
                    (select
                      block,
                      fields->>'fee' as fee,
                      hash,
                      type,
                      fields->>'nonce' as nonce,
                      fields->>'payer' as payer,
                      row_to_json(json_populate_recordset(null::transactions_payment, (fields->>'payments')::json)) as payments_list
                    from transactions
                    where transactions.type = 'payment_v2' and block >= {block_min} and block < {block_max}) 
                
                  select
                    block,
                    fee,
                    hash,
                    type,
                    nonce,
                    payments_list->>'payee' as payee,
                    payer,
                    (payments_list->>'amount')::bigint as amount
                  from payment_v2_parsed;""")
                columns_list = ["block", "fee", "hash", "type", "nonce", "payee", "payer", "amount"]
                output_path = f"transactions_payment_v2_{str(block_min).zfill(8)}-{str(block_max - 1).zfill(8)}.csv"
                convert_result_to_csv(res, columns_list, output_path)
                upload_to_s3_hook(output_path, f"transactions_payment_v2/{output_path}.gz")
                os.remove(output_path)
                os.remove(f"{output_path}.gz")

            except ValueError:
                # no results for query
                pass
            print(f"{block_min} to {block_max}")
            block_min = block_max
            block_max += LEDGER_DUMP_CHUNK_SIZE_BLOCKS
        dump_height = block_min
        return int(dump_height)


    @task(task_id="dump_blocks")
    def dump_blocks(current_height: int, dump_height: int):
        block_min = int(dump_height)
        block_max = int(block_min + LEDGER_DUMP_CHUNK_SIZE_BLOCKS)
        while (current_height - block_min) > LEDGER_DUMP_CHUNK_SIZE_BLOCKS:
            res = get_data_from_postgres(f"""SELECT height, time, timestamp, prev_hash, block_hash, transaction_count,
            hbbft_round, election_epoch, epoch_start, rescue_signature, snapshot_hash, created_at
            FROM blocks where height >= {block_min} AND height < {block_max};""")
            columns_list = ["height", "time", "timestamp", "prev_hash", "block_hash", "transaction_count",
                            "hbbft_round", "election_epoch", "epoch_start", "rescue_signature", "snapshot_hash", "created_at"]
            output_path = f"blocks_{str(block_min).zfill(8)}-{str(block_max - 1).zfill(8)}.csv"
            convert_result_to_csv(res, columns_list, output_path)
            upload_to_s3_hook(output_path, f"blocks/{output_path}.gz")
            block_min = block_max
            block_max += LEDGER_DUMP_CHUNK_SIZE_BLOCKS
            print(f"{block_min} to {block_max}")
        dump_height = block_min
        return int(dump_height)


    @task(task_id="save_status_local")
    def update_status_local(transactions_payment_v1_dump_height,
                            transactions_payment_v2_dump_height,
                            challenge_receipts_parsed_dump_height,
                            packets_dump_height,
                            rewards_dump_height,
                            blocks_dump_height):
        updated_status_dict = {
            "blocks": blocks_dump_height,
            "rewards": rewards_dump_height,
            "packets": packets_dump_height,
            "challenge_receipts_parsed": challenge_receipts_parsed_dump_height,
            "transactions_payment_v1": transactions_payment_v1_dump_height,
            "transactions_payment_v2": transactions_payment_v2_dump_height
        }
        with open("status.json", "w") as f:
            json.dump(updated_status_dict, f)
        return True

    payment_v1 = dump_transactions_payment_v1(current_height, status_dict["transactions_payment_v1"])
    payment_v2 = dump_transactions_payment_v2(current_height, status_dict["transactions_payment_v2"])
    challenge_receipts = dump_challenge_receipts_parsed(current_height, status_dict["challenge_receipts_parsed"])
    packets = dump_packets(current_height, status_dict["packets"])
    rewards = dump_rewards(current_height, status_dict["rewards"])
    blocks = dump_blocks(current_height, status_dict["blocks"])

    update_status = update_status_local(payment_v1,
                                        payment_v2,
                                        challenge_receipts,
                                        packets,
                                        rewards,
                                        blocks)
    upload_status = upload_to_s3_operator("upload_status_to_s3", "status.json", "status.json", compressed=False)

    [payment_v1, payment_v2, blocks] >> packets >> rewards >> challenge_receipts >> update_status >> upload_status



# ONLY DUMP INVENTORIES FOR NOW

inventory_dumps = etl_inventory_dumps()
# ledger_dumps = etl_ledger_dumps()