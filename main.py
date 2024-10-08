from fastapi import FastAPI, Query
from typing import List, Dict, Union, Optional
from urllib.parse import unquote
import jieba
import jieba.posseg as pseg

jieba.load_userdict("Ref/number.txt")
jieba.load_userdict("Ref/area.txt")
jieba.load_userdict("Ref/placename.txt")
jieba.load_userdict("Ref/Street_csdi.txt")
jieba.load_userdict("Ref/Street_data.txt")
jieba.load_userdict("Ref/Building_nt.txt")
jieba.load_userdict("Ref/Building_kh.txt")

app = FastAPI()

areas = {
    '香港': {'中西區': ['堅尼地城', '石塘咀', '西營盤', '上環', '中環', '金鐘', '半山', '山頂'],
             '灣仔': ['灣仔', '銅鑼灣', '跑馬地', '大坑', '掃桿埔', '渣甸山'],
             '東區': ['天后', '寶馬山', '北角', '鰂魚涌', '西灣河', '筲箕灣', '柴灣', '小西灣'],
             '南區': ['薄扶林', '香港仔', '鴨脷洲', '黃竹坑', '壽臣山', '淺水灣', '舂磡角', '赤柱', '大潭', '石澳']},
    '九龍': {'油尖旺': ['尖沙咀', '油麻地', '西九龍', '京士柏', '旺角', '大角咀'],
             '深水埗': ['美孚', '荔枝角', '長沙灣', '深水埗', '石硤尾', '又一村', '大窩坪', '昂船洲'],
             '九龍城': ['紅磡', '土瓜灣', '馬頭角', '馬頭圍', '啟德', '九龍城', '何文田', '九龍塘', '筆架山'],
             '黃大仙': ['新蒲崗', '黃大仙', '東頭', '橫頭磡', '樂富', '鑽石山', '慈雲山', '牛池灣'],
             '觀塘': ['坪石', '九龍灣', '牛頭角', '佐敦谷', '觀塘', '秀茂坪', '藍田', '油塘', '鯉魚門']},
    '新界': {'葵青': ['葵涌', '青衣'],
             '荃灣': ['荃灣', '梨木樹', '汀九', '深井', '青龍頭', '馬灣', '欣澳'],
             '屯門': ['大欖涌', '掃管笏', '屯門', '藍地'],
             '元朗': ['洪水橋', '廈村', '流浮山', '天水圍', '元朗', '新田', '落馬洲', '錦田', '石崗', '八鄉'],
             '北區': ['粉嶺', '聯和墟', '上水', '石湖墟', '沙頭角', '鹿頸', '烏蛟騰'],
             '大埔': ['大埔墟', '大埔', '大埔滘', '大尾篤', '船灣', '樟木頭', '企嶺下'],
             '沙田': ['大圍', '沙田', '火炭', '馬料水', '烏溪沙', '馬鞍山'],
             '西貢': ['清水灣', '西貢', '大網仔', '將軍澳', '坑口', '調景嶺', '馬游塘'],
             '離島': ['長洲', '坪洲', '大嶼山', '東涌', '南丫島']}
}

road_related_words = ['路', '道', '街', '巷', '橋', '隧道', '大道', '高速公路', '公路', '馬路', '徑']

def segment_input(input_str: str) -> Dict[str, Union[str, List[str], None]]:
    decoded_input = unquote(input_str)
    words = pseg.cut(decoded_input)

    street_name, street_number = [], []
    district, sub_district, area, building_details = None, None, None, ''
    number_found = False

    for word, flag in words:
        if any(road_word in word for road_word in road_related_words):
            street_name.append(word)
        elif (flag == 'm' or '號' in word) and not number_found:
            street_number.append(word)
            if '號' in word:
                number_found = True
                street_number[-1] = street_number[-1].split('號')[0] + '號'
        elif word in [sd for districts in areas.values() for sd in [k for k, v in districts.items()] + [item for sublist in districts.values() for item in sublist]]:
            for area_name, districts in areas.items():
                for district_name, sub_districts in districts.items():
                    if word == district_name:
                        district = word
                        area = area_name
                        break
                    elif word in sub_districts:
                        sub_district = word
                        district = district_name
                        area = area_name
                        break
                if area:
                    break

    if street_name or street_number:
        building_details = decoded_input.split(''.join(street_name) + ''.join(street_number), 1)[-1].strip()

    return {
        'area': area,
        'district': district,
        'sub_district': sub_district,
        'street_name': ' '.join(street_name) if street_name else None,
        'street_number': ' '.join(street_number) if street_number else None,
        'building': building_details if building_details else None
    }

@app.get("/seg")
async def segment_input_text(
    q: str = Query(..., description="The input text to be segmented"),
    item: Optional[List[str]] = Query(None, description="Specific items to return (a for area, d for district, sd for sub_district, st for street_name, sn for street_number, b for building)")
):
    try:
        segmented_output = segment_input(q)
        
        # Mapping query terms to actual fields
        item_mapping = {
            'a': 'area',
            'd': 'district',
            'sd': 'sub_district',
            'st': 'street_name',
            'sn': 'street_number',
            'b': 'building'
        }
        
        if item:
            # Translate short terms to full terms and filter the output
            filtered_output = {item_mapping[short_term]: segmented_output[item_mapping[short_term]] for short_term in item if short_term in item_mapping}
            return {"segmented_text": filtered_output}
        else:
            # Default to returning all items if no specific query terms are provided
            return {"segmented_text": segmented_output}
    except Exception as e:
        return {"error": str(e)}
