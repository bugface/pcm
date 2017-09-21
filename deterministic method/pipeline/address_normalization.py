import csv
import usaddress
import re
import time
import sys
import csv
import time
from geopy.geocoders import Nominatim, GeocoderDotUS, GoogleV3, Bing

#BING API_KEY
BING_API_KEY = "AhHdobtqQ-mhLgtzjqrt9bcZf_P3ON7ioCf0MsU-lbNuDMHQRSqPkBZTUqPpiM63"

# pre config for process address
# street name pattern
pat1 = re.compile('^[0-9]+[DHNRST]{2}[STAVEDR]*$')
pat2 = re.compile("^[0-9]+$")
pat11 = re.compile("^[0-9]+STR$")
pat3 = re.compile("^[0-9]+\s+[0-9]+[DHNRST]*$")
pat5 = re.compile("^[A-Z]+[0-9]+[DHNRST]*$")
# p7: '38 BORKEL' this has been matched but not processed correctly
pat7 = re.compile("^[0-9]+[A-Z]*\s+[A-Z]+$")
#pat8 = re.compile("^[0-9]+\s[DHNRST]*\s*[A-Z]*$")
pat8 = re.compile("^[0-9]+\s[DHNRST]*$")
pat10 = re.compile("^[0-9]+[DHNRST]{2}[0-9]+$")
# street number pattern
pat4 = re.compile("^[0-9]+[A-Z]+$")
pat6 = re.compile("^[0-9]+-[0-9]+$")
pat9 = re.compile("^[0-9]+\s[0-9]+$")

numeric2num = {
    'THIRD': '3',
    'SIXTH': '6',
    'FIFTH': '5',
    'SEVENTH': '7',
    'SEVENTEENTH': '17',
    'FOURTH': '4',
    'TWELFTH': '12',
    'TENTH': '10',
    'ELEVENTH': '11',
    'NINETEENTH': '19',
    'FIFTEENTH': '15',
    'THIRTEENTH': '13',
    'NINTH': '9',
    'FOURTEENTH': '14',
    'EIGHTEENTH': '18',
    'SIXTEENTH': '16',
    'EIGHTH': '8',
    'SECOND': '2',
    'FIRST': '1'
}

numdict = {
    "one": 1, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6, "seven": 7, "eight": 8,
    "nine": 9, "ten": 10, "eleven": 11, "twelve": 12, "thirteen": 13, "fourteen": 14, "fifteen": 15,
    "sixteen": 16, "seventeen": 17, "eighteen": 18, "nineteen": 19
}

road2abbv = {
    "AVENUE": "AVE",
    "MOUNT": "MT"
}

addvdic = {
    'CONC': 'CONCOURSE',
    'PT': 'POINT',
    'BWAY': 'BROADWAY',
    'GR': "GRAND",
    'GRAN': "GRAND",
    'PLNS': 'PLAINS',
    'PL': 'PLAINS',
    'SAINT': "ST",
    "ISLAND": "IS",
}

direction2abbv = {
    "WEST": "W",
    "EAST": "E",
    "SOUTH": "S",
    "NORTH": "N",
    "SOUTHEAST": "SE",
    "SOUTHWEST": "SW",
    "NORTHEAST": "NE",
    "NORTHWEST": "NW"
}

post_type = ['ST.', 'ST', 'STREET', 'AVE', 'AVENUE', 'PKWY', 'WAY', 'PL',
             'ROAD', 'RD', 'PLZ', 'HWY', 'TER', 'TERR', 'STREE', 'PLACE',
             'BLVD', 'BOULEVARD', 'DRIVE', 'DR', 'CIRCLE', 'AV', 'LOOP',
             'CIR', 'HTS', 'CT', "PATH", "LN", 'LANE', 'BVLD', 'LAN', 'BLUD']

# helper functions


def contain_digit(s):
    for each in s:
        if each.isdigit():
            return True
    return False

def normalize_address(addr_from_csv):
    raw_an = None
    raw_sn = None
    raw_snpd = ""
    pure_an = ""
    pure_sn = ""
    pure_pre_decorator = ""
    extra_for_sn = ""
    extra_for_predecorator = ""

    # print(row['ADDRESS1'])
    try:
        naddr = usaddress.tag(addr_from_csv)
    except:
        # #res = ""
        # continue
        return ""

    dic = naddr[0]
    # print(dic)

    # process address number
    '''
    question: some address numbe match a pattern like "B123" or "123A",
    should we care about the character in front of or after the number?
    (the case that the pre decorator like 'W' 'EAST' has already be taken care of)
    '''
    if 'AddressNumber' in dic:
        raw_an = dic['AddressNumber']
        if raw_an.lower() in numdict:
            pure_an += str(numdict[raw_an.lower()])
        elif contain_digit(pure_an):
            pure_an += re.findall("[0-9]+", raw_an)[0]
            if pat4.match(raw_an):
                s = re.findall("[A-Z]+", raw_an)[0]
                if s in ['W', 'S', 'E', 'N'] or s in direction2abbv:
                    extra_for_sn += s
            elif pat6.match(raw_an):
                pure_an += re.findall("[0-9]+", raw_an)[1]
            elif pat9.match(raw_an):
                #pure_an += raw_an.replace(" ", "")
                pure_an += re.findall("[0-9]+", raw_an)[1]
        else:
            pure_an = raw_an
    if pure_an[0:1] == '0':
        pure_an = pure_an[1:]

    # process address street name
    if 'StreetName' in dic:
        raw_sn = extra_for_sn + dic['StreetName']

        if "'" in raw_sn:
            i = raw_sn.index("'")
            raw_sn = raw_sn[:i] + raw_sn[i + 1:]

        if pat1.match(raw_sn) or pat2.match(raw_sn) or pat11.match(raw_sn):
            pure_sn += re.findall("[0-9]+", raw_sn)[0]
        elif pat3.match(raw_sn):
            nums = raw_sn.split(" ")
            pure_an += nums[0]
            pure_sn += re.findall("[0-9]+", nums[1])[0]
            if pure_sn == '':
                pure_sn += nums[1]
        elif raw_sn in numeric2num:
            pure_sn += numeric2num[raw_sn]
        elif raw_sn in addvdic:
            pure_sn += addvdic[raw_sn]
        elif pat5.match(raw_sn):
            index = 0
            for i, char in enumerate(raw_sn):
                if char.isdigit():
                    index = i
                    break
            extra_for_predecorator = raw_sn[:index]
            pure_sn += re.findall("[0-9]+", raw_sn[index:])[0]
        elif pat8.match(raw_sn):
            nums = raw_sn.split(" ")
            pure_sn += nums[0]
        elif pat7.match(raw_sn):
            '''
            the extra information cannot be decided if they belongs to
            the other part or it is a part of the street name that cannot be omitted
            current process is directly remove it
            '''
            p7_parts = raw_sn.split(" ")
            pure_sn += re.findall("[0-9]+",  p7_parts[0])[0]
            if p7_parts[1] not in post_type:
                pure_sn += p7_parts[1]
#                 sec = raw_sn.split(" ")
#                 help_pat1 = re.compile("^[]$")
        else:
            if " " in raw_sn:
                idx = raw_sn.index(" ")
                fir = raw_sn[:idx]
                sec = raw_sn[idx + 1:]
                if fir in addvdic:
                    fir = addvdic[fir]
                # print(sec)
                #print(sec in addvdic)
                pure_sn += fir
                if sec in addvdic:
                    #pure_sn += fir
                    sec = addvdic[sec]
                elif sec in post_type:
                    #pure_sn += fir
                    sec = ""
                # elif sec in [addvdic[x] for x in addvdic]:
                    #pure_sn += fir
                    #pure_sn += sec
                # else:
                    #pure_sn += raw_sn.replace(" ", "")
                pure_sn += sec
            else:
                pure_sn += raw_sn

    elif extra_for_sn != "":
        pure_sn += extra_for_sn

    # process street pre decorator
    '''
    question: what does 'ST in front of a name of street mean? st == saint?'
    '''
    flag = False
    if 'StreetNamePreDirectional' in dic:
        raw_snpd = dic['StreetNamePreDirectional']
#             if pat1.match(raw_snpd):
#                 flag = True
    # not handle st as saint problem
    elif extra_for_predecorator != "":
        raw_snpd = extra_for_predecorator

    if raw_snpd in direction2abbv:
        raw_snpd = direction2abbv[raw_snpd]

    if 'StreetNamePostDirectional' in dic:
        val = [direction2abbv[x] for x in direction2abbv]
        if dic['StreetNamePostDirectional'] in direction2abbv:
            pure_sn += direction2abbv[dic['StreetNamePostDirectional']]
        elif dic['StreetNamePostDirectional'] in val:
            pure_sn += dic['StreetNamePostDirectional']

    # process StreetNamePreType
    if 'StreetNamePreType' in dic:
        val = [road2abbv[x] for x in road2abbv]
        if dic['StreetNamePreType'] in road2abbv:
            pure_sn = road2abbv[dic['StreetNamePreType']] + pure_sn
        elif dic['StreetNamePreType'] in val:
            pure_sn = dic['StreetNamePreType'] + pure_sn

    # process StreetNamePostType:
    if 'StreetNamePostType' in dic:
        if dic['StreetNamePostType'] not in post_type:
            if " " in dic['StreetNamePostType']:
                pt_parts = dic['StreetNamePostType'].split(" ")
                for each in pt_parts:
                    if each not in post_type:
                        pure_sn += each
            else:
                pure_sn += dic['StreetNamePostType']

    res = "".join([pure_an, raw_snpd, pure_sn])
    if pat10.match(res):
        # print("!!!!!!!!")
        res = re.findall("[0-9]+", res)[0]
    elif res.endswith('AVENUEE') or res.endswith('AVENUIE'):
        res = res[:-7]
#         print(res)

    # process occupyidentifier
    if 'OccupancyIdentifier' in dic:
        pat_occ_1 = re.compile("^[0-9]+[EWNS]{1}\s[0-9]+[DHNRST]*$")
        pat_occ_2 = re.compile("^[0-9]+[DHNRST]*\s[A-Z]+[0-9]*[A-Z]*")
        pat_occ_3 = re.compile("^[0-9]+[EWNS]{1}[0-9]+[DHNRST]*$")
        if pat_occ_1.match(dic['OccupancyIdentifier']):
            parts = dic['OccupancyIdentifier'].split(' ')
            res += parts[0]
            res += re.findall("[0-9]+", parts[1])[0]
        elif pat_occ_2.match(dic['OccupancyIdentifier']):
            parts = dic['OccupancyIdentifier'].split(' ')
            res += re.findall("[0-9]+", parts[0])[0]
        elif pat_occ_3.match(dic['OccupancyIdentifier']):
            # print(1)
            m = re.search("^[0-9]+[EWNS]{1}[0-9]+", dic['OccupancyIdentifier'])
            # print(m)
            if m:
                res += m.group(0)
    return res

def geocoding(addr, flag='b'):
    res = None
    if flag == 'b':
        locator = Bing(BING_API_KEY)
        na = locator.geocode(query=addr, timeout=10, culture='en')
        if na is not None:
            q = na.address.split(",")
            n = len(q)
            if n < 4:
                res = addr
            else:
                res = ",".join([q[0], q[1]])
        else:
            res = addr
    elif flag == 'g':
        locator = GoogleV3()
    elif flag == 'n':
        locator = Nominatim()

    return res


#bing api key=AhHdobtqQ-mhLgtzjqrt9bcZf_P3ON7ioCf0MsU-lbNuDMHQRSqPkBZTUqPpiM63
#with open("processed_full_cover_detail.csv", "r") as f:
# with open("56626_ma.csv", "w", newline='') as f:
#     with open("56626.csv", "r") as f1:
#         writer = csv.writer(f)
#         reader = csv.DictReader(f1)
#         fields = reader.fieldnames
#         f
#         writer.writerow(fields)
#         for each in reader:
#             to_write = []
#             time.sleep(1)
#             #print(locator.geocode(query=(each['ADDRESS1'] + ' ' + each['CITY']), timeout=5))
#             #google v3
#             #na = locator1.geocode(query=(each['ADDRESS1'] + ' ' + each['CITY'] + ' USA'), timeout=5)

#             #use for bing
#

#             for k, v in each.items():
#                 to_write.append(v)
#             ##use for bing
#             if n <= 3:
#                 to_write.append(each['ADDRESS1'])
#             else:
#                 to_write.append(na.address)
#             ##used for google v3
#             # if na is not None:
#             #     to_write.append(na.raw['place_id'])
#             # else:
#             #     to_write.append(each['ADDRESS1'])
#             writer.writerow(to_write)
