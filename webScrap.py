from urllib.request import urlopen
from datetime import datetime, timedelta
import re
import requests

try:
  file = urlopen(r"http://10.85.20.8/maj/rsfilemanager.log")
except requests.Timeout as err:
  print("DEU ERRO AQUI " + err)

important = []
keep_phrases = ["Finalizando importacao lote"]

max_timed_out = timedelta(minutes=10)


for line in file:
  for phrase in keep_phrases:
    if phrase in line.decode("utf-8"):
      important.append(line.decode("utf-8"))
      break

log_text = important[len(important)-1]
matches = re.findall(r'(\d+/\d+/\d+ \d+:\d+:\d+)',log_text)

date_time_str = matches[0]
date_time_obj = datetime.strptime(date_time_str, '%Y/%m/%d %H:%M:%S')

date_time_difference =  datetime.today() - date_time_obj

print('Data atual: ', datetime.today() )
print('Dia do ultimo log: ',date_time_str)
print('Tempo entre o ultimo log e a data atual: ', date_time_difference)
print(log_text)
print('\n')
if (date_time_difference >= max_timed_out):
  print("Erro, tempo entre ultimo log e data atual maior que: ", max_timed_out, )
