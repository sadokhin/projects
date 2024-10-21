import asyncio
import datetime
import json

import google
import pandas
import pandas_gbq
import requests
import time
import traceback

from airflow import models
from airflow.operators.python_operator import PythonOperator
from dateutil import parser
from google.cloud import bigquery
from urllib.parse import quote_plus

from pandas_gbq.schema import to_google_cloud_bigquery
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from common.common_function import task_fail_telegram_alert

################################## Parameters. Don't edit it! ##################################
START_DATE_FROM = datetime.datetime(2022, 8, 1, 0, 0, 0, 0)
SOURCE_NAME = 'bitrix_deal'
DAG_NUMBER = '3'
YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)
LOCATION = 'europe-west3'
PROJECT = 'southern-idea-345503'
DATASET = 'raw_data_bitrix'
TABLE = 'deal_daily_copy'
TABLE_NAME = f'{PROJECT}.{DATASET}.{TABLE}'
BX_WH_URL = models.Variable.get('BX_WH_URL_Contacts_1_secret')
BX_PAGE_SIZE = 50
BX_BATCH_COMMANDS = 50
MAX_RUN_ITERATIONS = 100

credentials, project = google.auth.default(
    scopes=[
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/cloud-platform",
    ]
)
client = bigquery.Client(credentials=credentials,project=project)

default_args = {
    'owner': 'Composer Example',
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1),
    'on_failure_callback': task_fail_telegram_alert
}

################################## Parametrs. Don't edit it! ##################################
with models.DAG(
    dag_id=f'{DATASET}_{SOURCE_NAME}_dag_{DAG_NUMBER}',
    default_args=default_args,
    catchup=False,
    start_date=YESTERDAY,
    schedule_interval='15 0-23 * * *',
    tags = ['load','bitrix','api'],
) as dag:

    def _call_bx_method(method, data):
        def _call_bx(method, data):
            return requests.post(
                url='{}/{}/'.format(BX_WH_URL, method),
                data=data
            )

        response = None
        call_delay = 1
        while call_delay < 3:
            print(call_delay)
            try:
                response = _call_bx(method, data)
                if response.ok and response.json():
                    return response
                else:
                    print(response.status_code)
                    print(response.content)
            except:
                tb = traceback.format_exc()
                print(tb)
                pass
            time.sleep(call_delay)
            call_delay += call_delay
        return response

    async def item_list_batch(start_date_modify, filters, fields, page_idx=0):
        data = []
        data.append('halt=0')

        while page_idx < BX_BATCH_COMMANDS:
            cmd = 'cmd[page_{}]=crm.deal.list'.format(page_idx)
            cmd_args = []
            for idx, field in enumerate(fields):
                cmd_args.append('select[{}]={}'.format(idx, field))
            for key, value in filters.items():
                cmd_args.append('filter[{}]={}'.format(key, value))
            cmd_args.append('order[DATE_MODIFY]=ASC')
            cmd_args = quote_plus('?' + '&'.join(cmd_args))
            if page_idx == 0:
                if start_date_modify:
                    cmd_args += quote_plus('&filter[>DATE_MODIFY]=') + str(start_date_modify)
            else:
                cmd_args += quote_plus('&filter[>DATE_MODIFY]=') + "$result[page_{}][{}][DATE_MODIFY]".format(
                    page_idx - 1, BX_PAGE_SIZE - 1)
            # if page_idx > 0:
            #     cmd_args += quote_plus('&start={}'.format(page_idx * BX_PAGE_SIZE))
            cmd_args += quote_plus('&start=-1')
            data.append(cmd + cmd_args)
            page_idx += 1

        data = '&'.join(data)
        response = _call_bx_method('batch', data)
        response_data = response.json()
        items = []
        for cmd_key, cmd_result in response_data.get('result', {}).get('result', {}).items():
            for item in cmd_result:
                if not next((_ for _ in items if _['ID'] == item['ID']), None):
                    items.append(item)
        return items

    def get_main_last_item():
        try:
            result = client.query(f"SELECT MAX(DATE_MODIFY) FROM `southern-idea-345503.raw_data_bitrix.deal`").result()
            return next(result)[0]
        except google.api_core.exceptions.NotFound:
            return None


    def get_last_item():
        try:
            result = client.query(f"SELECT MAX(DATE_MODIFY) FROM `{TABLE_NAME}`").result()
            return next(result)[0]
        except google.api_core.exceptions.NotFound:
            return None

    def load_items():
        iteration = 0
        while iteration < MAX_RUN_ITERATIONS:
            iteration += 1
            print('iteration')
            print(iteration)

            last_item = get_last_item()
            last_item_main = get_main_last_item()
            print('last_item')
            print(last_item)
            date_modify = last_item if last_item else last_item_main
            date_modify = date_modify - datetime.timedelta(seconds=30)
            date_modify = date_modify.strftime('%Y-%m-%dT%H:%M:%S.%f%Z')

            filters = {
                '>DATE_MODIFY': date_modify
            }
            print('filters')
            print(filters)

            fields = [
                'ID', 'TITLE', 'TYPE_ID', 'CATEGORY_ID', 'STAGE_ID', 'STAGE_SEMANTIC_ID', 'IS_NEW', 'IS_RECURRING',
                'IS_RETURN_CUSTOMER', 'IS_REPEATED_APPROACH', 'PROBABILITY', 'CURRENCY_ID', 'OPPORTUNITY',
                'IS_MANUAL_OPPORTUNITY', 'TAX_VALUE', 'COMPANY_ID', 'CONTACT_ID', 'CONTACT_IDS', 'QUOTE_ID', 'BEGINDATE',
                'CLOSEDATE', 'OPENED', 'CLOSED', 'COMMENTS', 'ASSIGNED_BY_ID', 'CREATED_BY_ID', 'MODIFY_BY_ID',
                'MOVED_BY_ID', 'DATE_CREATE', 'DATE_MODIFY', 'MOVED_TIME', 'SOURCE_ID', 'SOURCE_DESCRIPTION', 'LEAD_ID',
                'ADDITIONAL_INFO', 'LOCATION_ID', 'ORIGINATOR_ID', 'ORIGIN_ID', 'UTM_SOURCE', 'UTM_MEDIUM', 'UTM_CAMPAIGN',
                'UTM_CONTENT', 'UTM_TERM', 'UF_INTERNAL_REF', 'UF_CRM_5D08C6358C001', 'UF_CRM_1560856268',
                'UF_CRM_1560856359', 'UF_CRM_1560856409', 'UF_CRM_1560856425', 'UF_CRM_1560856538', 'UF_CRM_1560856847',
                'UF_CRM_1560856878', 'UF_CRM_1560856890', 'UF_CRM_1560856910', 'UF_CRM_1560856940', 'UF_CRM_1560857001',
                'UF_CRM_1560857206', 'UF_CRM_1560857236', 'UF_CRM_1560857262', 'UF_CRM_1560857403', 'UF_CRM_1561204895',
                'UF_CRM_5D0F0DD712B7C', 'UF_CRM_5D0F0DD719ABC', 'UF_CRM_1563363025076', 'UF_CRM_1563697852',
                'UF_CRM_1564311644', 'UF_CRM_1564571662858', 'UF_CRM_5D4D681DE8A74', 'UF_CRM_5D4D681E15662', 'UF_TAG_TEXT',
                'UF_TAG', 'UF_CRM_5D677E38B4297', 'UF_CRM_5D677EB852B1A', 'UF_CRM_5D677EB87B2F3', 'UF_CRM_5D6A7B362362C',
                'UF_CRM_5D6B83FFBBC0D', 'UF_CRM_5D6BA743CF80B', 'UF_CRM_5D6E8B06D8890', 'UF_CRM_1568638711263',
                'UF_CRM_1568638741710', 'UF_CRM_1568638819', 'UF_CRM_1568638864', 'UF_CRM_1568639528475',
                'UF_CRM_5DA487736FD22', 'UF_CRM_1571214268069', 'UF_LEAD', 'UF_CRM_1580135027513', 'UF_CRM_1580722078686',
                'UF_CRM_1581571003268', 'UF_CRM_1581571301', 'UF_CRM_1581571312', 'UF_CRM_1582026478',
                'UF_CRM_5E68AABE48697', 'UF_CRM_5E707E4DD7D70', 'UF_CRM_5E804D7A40F6F', 'UF_CRM_5E804D7D42672',
                'UF_CRM_5E804D834FC40', 'UF_CRM_5E804D8F78537', 'UF_CRM_1615888955560', 'UF_CRM_1615889053353',
                'UF_CRM_1616352637252', 'UF_CRM_605991ED8060A', 'UF_CRM_605991F96498A', 'UF_CRM_1618993099',
                'UF_CRM_1619071365', 'UF_CRM_1619071453', 'UF_CRM_1619072032', 'UF_CRM_1619072147', 'UF_CRM_1619075663',
                'UF_CRM_1619075704', 'UF_CRM_1619076132', 'UF_CRM_1619076167', 'UF_CRM_1619076194', 'UF_CRM_1619786024',
                'UF_CRM_1619786647', 'UF_CRM_1619786766', 'UF_CRM_1619786850', 'UF_CRM_1619788719', 'UF_CRM_1619983564',
                'UF_CRM_1619983647', 'UF_CRM_1619983928', 'UF_CRM_1619983968', 'UF_CRM_1623226000547',
                'UF_CRM_60C8428142E17', 'UF_CRM_1625489296814', 'UF_CRM_61531BF1AE6EB', 'UF_CRM_615E84A233908',
                'UF_CRM_615E84C5D4E03', 'UF_CRM_BITCONF_LINK', 'UF_CRM_615E84E0D1769', 'UF_CRM_BITCONF_ZOOM_RECORDINGS',
                'UF_CRM_615E850E72925', 'UF_CRM_BITCONF_RECORDINGS_VIDEO_YANDEX', 'UF_CRM_BITCONF_RECORDINGS_AUDIO_YANDEX',
                'UF_CRM_615E853A898B2', 'UF_CRM_BITCONF_RECORDINGS_CHAT_YANDEX', 'UF_CRM_BITCONF_RECORDINGS_VIDEO_GOOGLE',
                'UF_CRM_615E857743E5B', 'UF_CRM_BITCONF_RECORDINGS_AUDIO_GOOGLE', 'UF_CRM_BITCONF_RECORDINGS_CHAT_GOOGLE',
                'UF_CRM_615E85BA57A84', 'UF_CRM_BITCONF_ZOOM_REGISTRATION_ANSWERS_JSON',
                'UF_CRM_BITCONF_ZOOM_REGISTRATION_ANSWERS', 'UF_CRM_BITCONF_MEETING_ATTENDED', 'UF_CRM_615E861AEECCA',
                'UF_CRM_BITCONF_MEETING_STARTED_AT', 'UF_CRM_BITCONF_MEETING_ENDED_AT', 'UF_CRM_615E86E3E5F10',
                'UF_CRM_615E875EA4A55', 'UF_CRM_61685415030BF', 'UF_CRM_6168542DD1869', 'UF_CRM_619B703A1E434',
                'UF_CRM_1639289229', 'UF_CRM_1639429359', 'UF_CRM_1640678789506', 'UF_CRM_1640678954352',
                'UF_CRM_1640679478738', 'UF_CRM_1640679678718', 'UF_CRM_1640679915596', 'UF_CRM_1640680049426',
                'UF_CRM_1640680147883', 'UF_CRM_1640761456', 'UF_CRM_1643719065974', 'UF_CRM_1643720663009',
                'UF_CRM_1643723691367', 'UF_CRM_1643724210179', 'UF_CRM_1643724374036', 'UF_CRM_1643725093',
                'UF_CRM_1643726177770', 'UF_CRM_1643726504393', 'UF_CRM_1643788137', 'UF_CRM_1644223273',
                'UF_CRM_1644232737119', 'UF_CRM_1644233103505', 'UF_CRM_1644233154819', 'UF_CRM_1644233222930',
                'UF_CRM_1644233844163', 'UF_CRM_1644233902368', 'UF_CRM_1644233961838', 'UF_CRM_1644234031221',
                'UF_CRM_1644234120209', 'UF_CRM_1644234193696', 'UF_CRM_1644234266141',
                'UF_CRM_BITCONF_PARTICIPANT_JOINED_AT', 'UF_CRM_620B63597A427', 'UF_CRM_BITCONF_PARTICIPANT_LEFT_AT',
                'UF_CRM_620B63823D8D4', 'UF_CRM_620B63A864A97', 'UF_CRM_620B63BF7AAE9', 'UF_CRM_620B63CDA15A8',
                'UF_CRM_620B64047ADD5', 'UF_CITY', 'UF_COMMUNITY', 'UF_PROP_BUILDING', 'UF_PROP_PROJECT',
                'UF_CRM_6245A6F0BCCDB', 'UF_CRM_6245A76C1F06F', 'UF_CRM_624C48458A3AF', 'UF_CRM_624DA9F020779',
                'UF_CRM_1650041106', 'UF_CRM_1653914068753', 'UF_CRM_1655382581389', 'UF_CRM_1655382672389',
                'UF_CRM_5DC11FFAB334E', 'UF_CRM_5DC11FFC56D42', 'UF_CRM_5DC11FFDDA570', 'UF_CRM_5DC11FFEAE5CC',
                'UF_CRM_5E805CB01295E', 'UF_CRM_1590646315', 'UF_CRM_1590837049', 'UF_CRM_1590606111', 'UF_CRM_1590839399',
                'UF_CRM_1590839565', 'UF_CRM_1590839039', 'UF_CRM_1590839368', 'UF_CRM_1590838961', 'UF_CRM_1590603986',
                'UF_CRM_1609408230', 'UF_CRM_1609195088', 'UF_CRM_1604324019', 'UF_CRM_1604411438', 'UF_CRM_1604324874',
                'UF_CRM_1609408261', 'UF_CRM_1609195127', 'UF_CRM_1604344267', 'UF_CRM_1609191142', 'UF_CRM_1604841418',
                'UF_CRM_1609196677', 'UF_CRM_1604344325', 'UF_CRM_1604344396', 'UF_CRM_1604324850', 'UF_CRM_1604344248',
                'UF_CRM_1609194981', 'UF_CRM_1604324902', 'UF_CRM_1604344375', 'UF_CRM_1604344345', 'UF_CRM_1604324838',
                'UF_CRM_1604344231', 'UF_CRM_1609196637', 'UF_CRM_1609194520', 'UF_CRM_1608823680', 'UF_CRM_1608823649',
                'UF_CRM_1607934054', 'UF_CRM_1607934335', 'UF_CRM_1604324752', 'UF_CRM_1604335666', 'UF_CRM_1607259422',
                'UF_CRM_1604324109', 'UF_CRM_1604334891', 'UF_CRM_1612191233', 'UF_CRM_1604341790', 'UF_CRM_1604327536',
                'UF_CRM_1604565906', 'UF_CRM_1604565983', 'UF_CRM_1608539525', 'UF_CRM_1604323289', 'UF_CRM_1604564303',
                'UF_CRM_1609191970', 'UF_CRM_1604323942', 'UF_CRM_1610973555', 'UF_CRM_1608818286', 'UF_CRM_1604859550',
                'UF_CRM_1609194505', 'UF_CRM_1583917559825', 'UF_CRM_5E81CCB476749', 'UF_CRM_1583054788381',
                'UF_CRM_5FCCCDDCDE6D6', 'UF_CRM_5DC11FB8D9320', 'UF_CRM_5DC11FB9DC110', 'UF_CRM_5DC11FBA9D088',
                'UF_CRM_5EBBD7E889576', 'UF_CRM_60E6066223AB2', 'UF_CRM_5FF2EC5719D16', 'UF_CRM_5FF2EC58B66A6',
                'UF_CRM_1597319027', 'UF_CRM_5F35277E39729', 'UF_CRM_5DC11FD352EEC', 'UF_CRM_5EBBD7E694FA8',
                'UF_CRM_5E804D9A8BFD1', 'UF_CRM_5D677E38DD998', 'UF_CRM_5F4362B7822EC', 'UF_CRM_5E804D975A9DC',
                'UF_CRM_1616405828810', 'UF_CRM_1616405785535', 'UF_CRM_5E8D9530B12BF', 'UF_CRM_5E5B80FCEB7EC',
                'UF_CRM_1627504220049', 'UF_CRM_605FBB1B3213C', 'UF_CRM_1638699033', 'UF_CRM_5DC11FD833E20',
                'UF_CRM_1616352810934', 'UF_CRM_1616353664162', 'UF_CRM_1616491049360', 'UF_CRM_1616353813755',
                'UF_CRM_1616396767334', 'UF_CRM_1616354017280', 'UF_CRM_1616491114961', 'UF_CRM_1616354046467',
                'UF_CRM_1616491020505', 'UF_CRM_1616353743113', 'UF_CRM_1616353943055', 'UF_CRM_1616411387762',
                'UF_CRM_1616352863380', 'UF_CRM_1616396716732', 'UF_CRM_1616411347249', 'UF_CRM_1616354083665',
                'UF_CRM_5E8D95325E84B', 'UF_CRM_5E8D9538EB88C', 'UF_CRM_5D9452DFD56AF', 'UF_CRM_5DA6D179E2912',
                'UF_CRM_1616320237', 'UF_CRM_1616320267', 'UF_CRM_1616320356', 'UF_CRM_1616320595', 'UF_CRM_1616321310',
                'UF_CRM_1616321372', 'UF_CRM_1616322556', 'UF_CRM_1616322624', 'UF_CRM_1616322930', 'UF_CRM_1616322965',
                'UF_CRM_1616576077', 'UF_CRM_1616576456', 'UF_CRM_1616662726', 'UF_CRM_1616662852', 'UF_CRM_1616672114',
                'UF_CRM_1616923225', 'UF_CRM_1616923353', 'UF_CRM_1617796112', 'UF_CRM_1640165504', 'UF_CRM_1660032210',
                'UF_CRM_VKONTAKTE', 'UF_CRM_6319B52AB591B', 'UF_CRM_TELEGRAM', 'UF_CRM_1662700273',
                'UF_CRM_I2CRM_CHAT', 'UF_CRM_TIKTOK', 'UF_CRM_INSTAGRAM', 'UF_CRM_6319B5F310A07', 'UF_CRM_1663067869', 
                'UF_CRM_1662625249', 'UF_CRM_1661942321', 'UF_CRM_1662971295', 'UF_CRM_1661943970', 'UF_CRM_1663057687', 
                'UF_CRM_1663067966', 'UF_CRM_1663067644', 'UF_CRM_1663068174', 'UF_CRM_1661936643', 'UF_CRM_1663648481',
                'UF_CRM_1665560153', 'UF_CRM_1665647859226', 'UF_CRM_1663236064', 'UF_CRM_1663124691', 'UF_CRM_1666261607435',
                'UF_CRM_1666762747', 'UF_CRM_1666592741', 'UF_CRM_1666777288141', 'UF_CRM_1667306736', 'PARENT_ID_187', 
                'UF_CRM_1667307603', 'UF_CRM_1667306903', 'UF_CRM_1667536633', 'UF_CRM_1668489321', 'UF_CRM_1668491130',
                'UF_CRM_1668527501', 'UF_CRM_URL', 'UF_CRM_URL_BB', 'LAST_ACTIVITY_BY', 'LAST_ACTIVITY_TIME',
                'UF_CRM_1670826985', 'UF_CRM_1670932823', 'UF_CRM_1670932932', 'UF_CRM_6399B9BF48F30', 'UF_CRM_1671066608', 
                'UF_CRM_1671077712', 'UF_CRM_1671077652', 'UF_CRM_1672213266', 'UF_CRM_1673516120', 'UF_CRM_63AAE5C75ABE4', 
                'UF_CRM_RS_UTM_CAMP', 'UF_CRM_63AAE75A33EB1', 'UF_CRM_RS_GOOGLE_CID', 'UF_CRM_RS_UTM_MEDIUM', 'UF_CRM_63AAE680D96FC', 
                'UF_CRM_RS_UTM_SOURCE', 'UF_CRM_RS_UTM_TERM', 'UF_CRM_63AAE6CFCB200', 'UF_CRM_63AAE7CD28F6B', 'UF_CRM_RS_UTM_CONT', 
                'UF_CRM_63AAE5892BEA9', 'UF_CRM_1674472783', 'UF_CRM_63D0E728A28D3', 'UF_CRM_1674718484', 'UF_CRM_1674711304478', 
                'UF_CRM_1674718582', 'UF_CRM_1675410262', 'UF_CRM_1676375692', 'UF_CRM_1676374947', 'UF_CRM_1676375931', 
                'UF_CRM_1676375236', 'UF_CRM_1676376073', 'UF_CRM_1676375384', 'UF_CRM_1676375076', 'UF_CRM_1676375810',
                'UF_CRM_63D0E3E9C22B0', 'UF_CRM_63D0E600E4AFC', 'UF_CRM_63D0DEF8E715D', 'UF_CRM_63D0DE36375A4', 
                'UF_CRM_63D0E145E5B9F', 'UF_CRM_63D0DD7CEBC76', 'UF_CRM_63D0E268398DB', 'UF_CRM_63D0E44148D9E', 
                'UF_CRM_63D0E3340E55F', 'UF_CRM_63D0E39067129', 'UF_CRM_63D0E204672BC', 'UF_CRM_63D0E1A5050C6', 
                'UF_CRM_63D0DDDA1AFF8', 'UF_CRM_63D0E541B16A2', 'UF_CRM_63D0E5A2C9771', 'UF_CRM_63D0E6C5A375C', 
                'UF_CRM_63D0E2D3B5EE8', 'UF_CRM_63D0E0E4E205A', 'UF_CRM_63D0DFBE75474', 'UF_CRM_63D0E4EC96849', 
                'UF_CRM_63D0E081139BE', 'UF_CRM_63D0E6637689E', 'UF_CRM_63D0E01CDD16E', 'UF_CRM_63D0DF5BD42B3', 
                'UF_CRM_63D0E497B8A05', 'UF_CRM_63D0DE95BB8DB', 'UF_CRM_1677226382', 'UF_CRM_1677234662826', 
                'UF_CRM_1677237966617', 'UF_CRM_1677235884691', 'UF_CRM_1677579951369', 'UF_CRM_1677581134091',
                'UF_CRM_1680163614','UF_CRM_1686044805', 'PARENT_ID_174', 'UF_CRM_1684306868', 'UF_CRM_SOURCE_CUSTOM', 'UF_CRM_1686045409', 'UF_CRM_1683112147',
                'UF_CRM_1685430687', 'UF_CRM_1684316126','UF_CRM_64A57C1BD8607','UF_CRM_1689189487','UF_CRM_1689232136','UF_BT_SOURCE','UF_CRM_1690436041','UF_SO_SOURCE',
                'UF_CRM_1688736530','UF_CRM_1688567624','UF_CRM_1688566812','UF_CRM_1689753152','UF_CRM_1689750590','UF_CRM_1689849726','UF_CRM_1689232909','UF_CRM_1687603656',
                'UF_CRM_1691506343','UF_CRM_1689188192','UF_CRM_1688566539','UF_CRM_1689865848','UF_CRM_1688735077','UF_CRM_1689745346','UF_CONTAC_BY_SELLER','UF_CRM_1688736906',
                'UF_CRM_1689188292','UF_CRM_1688735563','UF_CRM_UF_SELLER_SOURCE','UF_CRM_64A57C19AA820','UF_CRM_1689184210','UF_CONTAC_BY_CLIENT','UF_CRM_1688737120',
                'UF_CRM_1689750115','UF_CRM_1689746058','UF_CRM_1689745034','UF_CRM_1689185788','UF_CRM_1689185026','UF_CRM_1687791733','UF_CRM_1689188454','UF_CRM_1689656950',
                'UF_PROPERTY_OWNERS_FOLLOW_UP','UF_CRM_1689186570','UF_CRM_1689746307','UF_CRM_1689184689','UF_CRM_1689750355','UF_CRM_1688734847','UF_CRM_1689185593',
                'UF_CRM_1689234692','UF_CRM_1688735754','UF_CRM_1689186250','UF_CRM_1689184446','UF_CRM_1688195974','UF_CRM_1689225727','UF_CRM_1689186713','UF_CRM_1689254720',
                'UF_CRM_1688622883','UF_CRM_1689187783','UF_CRM_64A6CD85B4915','UF_CRM_1689847955','UF_CRM_1689186004','UF_CRM_1689231322','UF_CRM_6_1695379069','UF_CRM_6_1694170323','UF_CRM_1693927840'
            ]

            items = asyncio.run(
                item_list_batch(start_date_modify=date_modify, filters=filters, fields=fields, page_idx=0)
            )
            print(BX_WH_URL)
            print(len(items))

            simple_types = (str, int, float,)
            force_int_fields = (
                'ID', 'LEAD_ID', 'COMPANY_ID', 'CONTACT_ID', 'ASSIGNED_BY_ID', 'CREATED_BY_ID', 'MODIFY_BY_ID',
                'CATEGORY_ID', 'MOVED_BY_ID','UF_CRM_6_1694170323'
            )
            force_float_fields = (
                'OPPORTUNITY',
            )
            force_datetime_fields = (
                'DATE_CREATE', 'DATE_MODIFY', 'MOVED_TIME',
                'UF_CRM_5FCCCDDCDE6D6','UF_CRM_1693927840'  # Lead Added Date
            )
            force_date_fields = (
                'BEGINDATE',  # None
                'CLOSEDATE',  # None
                'UF_CRM_1560856538',  # Estimated transaction timing
                'UF_CRM_1568638819',  # Loyalty payment date
                'UF_CRM_1568638864',  # Refferal payment date
                'UF_CRM_1581571003268',  # Completion date of the project
                'UF_CRM_1582026478',  # Date of contract Signing
                'UF_CRM_5E68AABE48697',  # Lead Added Date (auto)
                'UF_CRM_605991F96498A',  # Date of Signing
                'UF_CRM_1604324752',  # Client's date of Birth
                'UF_CRM_1604324109',  # Deal Signing Date
                'UF_CRM_1604341790',  # Form F  Signing Date
                'UF_CRM_1609194505',  # Tenancy Contract Signing Date
                'UF_CRM_1583917559825',  # Lead Added Date
                'UF_CRM_1616923225',  # Booking form signing date
                'UF_CRM_1616923353',  # Agreement signing date
            )
            for item in items:
                for field, value in item.items():
                    if field in force_int_fields:
                        try:
                            item[field] = int(value)
                        except (TypeError, ValueError):
                            item[field] = None
                    elif field in force_float_fields:
                        try:
                            item[field] = float(value)
                        except (TypeError, ValueError):
                            item[field] = None
                    elif field in force_datetime_fields:
                        if value:
                            item[field] = parser.parse(value)
                        else:
                            item[field] = None
                    elif field in force_date_fields:
                        if value:
                            item[field] = parser.parse(value)
                        else:
                            item[field] = None
                    elif isinstance(value, simple_types):
                        item[field] = str(value)
                    else:
                        item[field] = json.dumps(value)
                for field in fields:
                    if field not in item:
                        item[field] = None
            print(len(items))
            src_table_schema = []
            for field in fields:
                if field in force_int_fields:
                    src_table_schema.append({'name': field, 'type': 'INTEGER'})
                elif field in force_float_fields:
                    src_table_schema.append({'name': field, 'type': 'FLOAT'})
                elif field in force_date_fields:
                    src_table_schema.append({'name': field, 'type': 'TIMESTAMP'})
                elif field in force_datetime_fields:
                    src_table_schema.append({'name': field, 'type': 'TIMESTAMP'})
                else:
                    src_table_schema.append({'name': field, 'type': 'STRING'})

            try:
                client.get_table(TABLE_NAME)  # Make an API request.
                print("Table {} already exists.".format(TABLE_NAME))
            except:
                schema = to_google_cloud_bigquery(dict(fields=src_table_schema))
                table = bigquery.Table(TABLE_NAME)
                table.schema = schema
                client.create_table(table)
            
            df = pandas.json_normalize(items)
            pandas_gbq.to_gbq(
                dataframe=df,
                destination_table=TABLE_NAME,
                project_id=PROJECT,
                table_schema=src_table_schema,
                if_exists='append',
            )

            if len(items) < BX_PAGE_SIZE * BX_BATCH_COMMANDS:
                break

        return True

    update_deals = BigQueryInsertJobOperator(
        task_id='update_deals',
        gcp_conn_id='google_cloud_default',
        configuration={
            "query": {
                "query":
                f"""
                    DELETE 
                    FROM `southern-idea-345503.raw_data_bitrix.deal`
                    WHERE ID IN (SELECT DISTINCT ID FROM `southern-idea-345503.raw_data_bitrix.deal_daily_copy`);

                    INSERT INTO `southern-idea-345503.raw_data_bitrix.deal`
                    SELECT * EXCEPT(RN) FROM (
                        SELECT 
                            DISTINCT *, ROW_NUMBER() OVER (PARTITION BY ID ORDER BY DATE_MODIFY DESC) as RN 
                        FROM  `southern-idea-345503.raw_data_bitrix.deal_daily_copy`
                    )
                    WHERE RN = 1;

                    TRUNCATE TABLE `southern-idea-345503.raw_data_bitrix.deal_daily_copy`;
                    
                """,
            "useLegacySql": False
            }
        }
    )


    load_items_task = PythonOperator(
        task_id='load_items_task',
        python_callable=load_items,
    )

    load_items_task >> update_deals