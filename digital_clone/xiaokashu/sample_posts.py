import sqlite3
conn = sqlite3.connect('digital_clone/xiaokashu/posts.db')
c = conn.cursor()

print('=== Posts WITHOUT 转债 (sample 20) ===')
rows = c.execute("""SELECT time_text, LENGTH(full_text), substr(full_text, 1, 120)
FROM posts WHERE full_text NOT LIKE '%转债%' ORDER BY time_text DESC LIMIT 20""").fetchall()
for t, l, txt in rows:
    print(f'[{t}] ({l}ch) {txt}')

print()
print('=== Stock-related without 转债 ===')
rows = c.execute("""SELECT time_text, LENGTH(full_text), substr(full_text, 1, 120)
FROM posts WHERE full_text NOT LIKE '%转债%'
AND (full_text LIKE '%股票%' OR full_text LIKE '%持仓%' OR full_text LIKE '%涨停%'
    OR full_text LIKE '%估值%' OR full_text LIKE '%买入%' OR full_text LIKE '%卖出%'
    OR full_text LIKE '%投资%' OR full_text LIKE '%收益%')
ORDER BY time_text DESC LIMIT 15""").fetchall()
for t, l, txt in rows:
    print(f'[{t}] ({l}ch) {txt}')

print()
print('=== Pure life/noise posts (no investment keywords) ===')
rows = c.execute("""SELECT time_text, LENGTH(full_text), substr(full_text, 1, 100)
FROM posts WHERE full_text NOT LIKE '%转债%'
AND full_text NOT LIKE '%股票%' AND full_text NOT LIKE '%持仓%'
AND full_text NOT LIKE '%涨停%' AND full_text NOT LIKE '%估值%'
AND full_text NOT LIKE '%买入%' AND full_text NOT LIKE '%卖出%'
AND full_text NOT LIKE '%投资%' AND full_text NOT LIKE '%收益%'
AND full_text NOT LIKE '%下修%' AND full_text NOT LIKE '%强赎%'
AND full_text NOT LIKE '%到期%' AND full_text NOT LIKE '%回售%'
AND full_text NOT LIKE '%正股%' AND full_text NOT LIKE '%转股%'
ORDER BY time_text DESC LIMIT 20""").fetchall()
for t, l, txt in rows:
    print(f'[{t}] ({l}ch) {txt}')

cnt = c.execute("""SELECT COUNT(*) FROM posts WHERE full_text NOT LIKE '%转债%'
AND full_text NOT LIKE '%股票%' AND full_text NOT LIKE '%持仓%'
AND full_text NOT LIKE '%涨停%' AND full_text NOT LIKE '%估值%'
AND full_text NOT LIKE '%买入%' AND full_text NOT LIKE '%卖出%'
AND full_text NOT LIKE '%投资%' AND full_text NOT LIKE '%收益%'
AND full_text NOT LIKE '%下修%' AND full_text NOT LIKE '%强赎%'
AND full_text NOT LIKE '%到期%' AND full_text NOT LIKE '%回售%'
AND full_text NOT LIKE '%正股%' AND full_text NOT LIKE '%转股%'""").fetchone()[0]
print(f'\nTotal pure noise posts: {cnt}/1078')
