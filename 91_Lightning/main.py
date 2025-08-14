import websocket

def lzw_decompress(codes):
    dict_size = 256
    dictionary = {i: chr(i) for i in range(dict_size)}
    
    result = []
    prev_code = codes[0]
    result.append(dictionary[prev_code])
    
    for code in codes[1:]:
        if code in dictionary:
            entry = dictionary[code]
        elif code == dict_size:
            entry = dictionary[prev_code] + dictionary[prev_code][0]
        else:
            raise ValueError(f"Bad LZW code: {code}")
        
        result.append(entry)
        dictionary[dict_size] = dictionary[prev_code] + entry[0]
        dict_size += 1
        prev_code = code
    
    return ''.join(result)


def on_message(ws, message):
    print("📥 Reçu brut :", repr(message))

    # Étape 1 : convertir les caractères en codes
    codes = [ord(c) for c in message]

    # Étape 2 : décompresser
    try:
        decompressed = lzw_decompress(codes)
        print("✅ Décompressé :", decompressed[:300])
    except Exception as e:
        print("❌ Erreur de décompression :", e)

def on_open(ws):
    print("✅ Connecté")
    ws.send('{"a":111}')  # message d'abonnement

ws = websocket.WebSocketApp("wss://ws1.blitzortung.org/",
                            on_message=on_message,
                            on_open=on_open)

"""
ws.run_forever()

"""
"""✅ Décompressé : {"time":1753179164534518300,"lat":34.875838,"lon":-100.804428,"alt":0,"pol":0,"mds":11640,"mcg":168,"status":0,"region":3,"sig":[{"sta":1703,"time":1906330,"lat":30.430454,"lon":-97.7873,"alt":260,"status":4},{"sta":1949,"time":2005682,"lat":39.642895,"lon":-104.73893,"alt":1778,"status":4},{"sta":2
"""


def lzw_decompress(codes):
    dict_size = 256
    dictionary = {i: chr(i) for i in range(dict_size)}
    
    result = []
    prev_code = codes[0]
    result.append(dictionary[prev_code])
    
    for code in codes[1:]:
        if code in dictionary:
            entry = dictionary[code]
        elif code == dict_size:
            entry = dictionary[prev_code] + dictionary[prev_code][0]
        else:
            raise ValueError(f"Bad LZW code: {code}")
        
        result.append(entry)
        dictionary[dict_size] = dictionary[prev_code] + entry[0]
        dict_size += 1
        prev_code = code
    
    return ''.join(result)

response = '{"time":1753Ĉ9164ĊĐ18300,"latĆ34.8ĉĔ8ĘlonĆ-1ĖĠ0442Ĥ"alĜ:ė"polĆĸmdsĆ1Ď4ľcgł6ĲstěuŁķĘregiħĝĘsiň:[ĀŌał703ĘĂĄł9063ĕĥěĝ0.4ĕĐ4ĥŗ:-97.7ġťĳĵĆ26ĸŠtŏĆ4},şōũ49Ŧăą:2Ė5Ŋ2ůĶ39.ďı95ŸĨźīğ7389ƁĴĶĈſřōƉŐƌƎ"ŠƄ9ƤƔŨƗŭŭ2ƁĚƞƠĮ1įǅŹŻğ582ż7ĘưƄĉƴŎƷƍƏšƗīĸŧƖ2Ŵĕ3ƥęŰ:ĕ.ǊǐƾęǍƤų1ŵĲǖǫǨƈƊć2ǜƺƐćǊƜāƕƄŴįǒƝű.0Ċ80ĲĦƧŻ5ų9469Ƿƃ:İǙƶł0ǿƻ:Ȑǔȅǀ250Ǥ21ȋƗƠƫĕŬƦĩǳǐƚȭǕȝǤȠǼīȤȁȫǰǢƄ65ƒ7ȄǆƋ1ơ886Ǌȷźțȍ0ǵƫȽĶɊǺƵɁǾƹȥȫŮȩǣſ55īȱ32.Ɋ4ţŷǱȔ9ɰĖŅ5ƓƂƞɵǻǛɤɄȘǡȆǫī1ȬǨɎȞŲĎĕȯɖŻɰƆūƟɝƼȄʁłɣǝłēȨɇǫţʊŃȱİųɔƟɽȓȸɰ3ţ796ʙǫŤɀŐʞȀǞīǄƿƖʱƮżʭǪǒȍŃŬʭǍŃŲȘǋǨǸƅƇɡʺɃʽĈǨʣ˂ƆȿǩĶʩșɫĊʓčĠʳʲʶıʵʼȡƗ˗ƼǧˀĝƭƫġʨŲɔİĮʓŃɐʴ27Țʶǵďʹʝ˰Ɨǥ˳ʎɊʊˬʍįǭȑɼǌɷ3.ȯȭ7ɵǸʱƁʜǽ̉ǒƳɨƋŴĔƚɮȖįʴʓɓǭſʿɾĝı̢̇ʃʽʋ̌ĐŤ7ēʨƪ48ȰɶȸɐɫƟ̥̞0Ȩ̡1ʻɥţɽʣșİ7̜ʨɐ4Ď8ɳ̯Žʊ3ďȜĶǤɽ̡Ƹʟ:ʳ͕ͅŪɸșɮ8̙ȯĈɍǍ͞ĠĈĒ˪˞͏ȣ̹űƘ̼čĎǵͳ͵͐ʱ̯ğ̜̈́6Ȩ˒̷́͐̉Ǌʘ̦Ȟč̮ͣ˟˴Ί1ǰʮźͺΏ͜ƯȾ2ʛ˕̈\u0382:Ŋ͔ʇƒɯ̓ȒǪƬΡɔ\u038dͻ̜ɼ;ͨέ˯ίǒ̶Κ5ǤŤŊɮğɔͶȒˌēǭƮƢʶ̈́ʀςͪʼʠ̌͘ψΑŭ̐ǪĐ.ƟďΞΤ-ɒǭǤϡʶʑ̷͑ɄǒͮʇɊ0ʴŅ˸ĠġɔͅϩĔϼɽ˒̪˭ɢ̣͘ȄʣɫǄɳʌϣŽϗɉˋɷ˹Ĕč̝ȝɫρǚƋ̣ɊƁЋψ8͚̎ȗƲ\u0378ȔЁɌĖ˞Ǹē̠ϙΗΑʆǀǐƬȑɵʍȬɱɚĉϿˌĮơƆ̿ΩĶƆϘНḤ̌ɓɵʣƆƆФ˅ƞȖŤģ̖ĩȐĠʱΒΪˬ̣ͩı˚ʇŬįŃηƞŽȴΑȨϩ͐˿ŴʳЙƱȫ̷ϚɥɸѣǀŊώŭȱƅ̙\u03a2ɸрЪɐ̜ĕǰЯάъѷȁ͐ţ̌ɌŬƚѭǪψ̙̎̃͆ƨǵž6ɦˑȝŪ͎ςϲǞǄϝΚƫɼƅϢˠ̬́φѮΫžțҬʶ̥Ѡ]ĘdeĚyʠơƦcĽƝӄķ}'

codes = [ord(c) for c in response]
print(lzw_decompress(codes))
