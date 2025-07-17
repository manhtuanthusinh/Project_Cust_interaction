import time

from june_july_mapping import result_june, result_july, df_all
from matplotlib import pyplot as plt
import webbrowser
import os

output_folder = "HTML_output"
os.makedirs(output_folder, exist_ok=True)

#toPandas()
data_june = result_june.toPandas()
data_july = result_july.toPandas()
data_all = df_all.toPandas()

#figure june
plt.figure(figsize=(20,8))
y = data_june['Category_T6'].value_counts().index
x = data_june['Category_T6'].value_counts().values
padding = 20
for i in range(len(x)):
    plt.text(x[i] + padding, i, str(x[i]) + '-' + (f'{(x[i] / (x.sum())) * 100:.1f}%'), ha='left', va='center', color = 'red')
plt.barh(y,x)
plt.xlabel('Số lượng user')
plt.ylabel('Category tháng 6')
plt.title('Category user xem tháng 6')
plt.gca().invert_yaxis()

plt.savefig(os.path.join(output_folder,'june.png'))

#figure july
plt.figure(figsize=(20,8))
y = data_july['Category_T7'].value_counts().index
x = data_july['Category_T7'].value_counts().values
padding = 20
for i in range(len(x)):
    plt.text(x[i] + padding, i, str(x[i]) + '-' + (f'{(x[i] / (x.sum())) * 100:.1f}%'), ha='left', va='center', color = 'red')
plt.barh(y,x)
plt.xlabel('Số lượng user')
plt.ylabel('Category tháng 7')
plt.title('Category user xem tháng 7')
plt.gca().invert_yaxis()

plt.savefig(os.path.join(output_folder,'july.png'))

#figure all
plt.figure(figsize=(20, 8))
top_changes = data_all['Category_change'].value_counts().head(20)
y = top_changes.index
x = top_changes.values
padding = 20
total = data_all['Category_change'].value_counts().sum()
for i in range(len(x)):
    plt.text(x[i] + padding, i, f'{(x[i] / total) * 100:.1f}%',
             ha='left', va='center', color='red')
plt.barh(y, x)
plt.xlabel('Số lượng user')
plt.ylabel('Category change')
plt.title('Top Category thay đổi của user từ tháng 6 đến tháng 7')
plt.gca().invert_yaxis()

print("================= Figures Ready to show ================= ")
time.sleep(3)
plt.savefig(os.path.join(output_folder,'all.png'))
webbrowser.open(f"file://{os.path.abspath('HTML_output/HTML_Visualise.html')}")
