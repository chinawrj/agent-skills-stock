import sqlite3

conn = sqlite3.connect('digital_clone/xiaokashu/posts.db')
c = conn.cursor()

total = c.execute('SELECT COUNT(*) FROM posts').fetchone()[0]
print(f'Total posts: {total}')

for threshold in [100, 200, 500, 1000, 2000, 5000]:
    cnt = c.execute(f'SELECT COUNT(*) FROM posts WHERE LENGTH(full_text) > {threshold}').fetchone()[0]
    print(f'  > {threshold} chars: {cnt}')

avg_len = c.execute('SELECT AVG(LENGTH(full_text)) FROM posts').fetchone()[0]
print(f'Avg text length: {avg_len:.0f} chars (~{avg_len*1.5:.0f} tokens)')

total_chars_300 = c.execute('SELECT SUM(LENGTH(full_text)) FROM (SELECT full_text FROM posts ORDER BY time_text DESC LIMIT 300)').fetchone()[0]
print(f'Top 300 posts total chars: {total_chars_300} (~{total_chars_300*1.5:.0f} tokens)')

total_chars_all = c.execute('SELECT SUM(LENGTH(full_text)) FROM posts').fetchone()[0]
print(f'All posts total chars: {total_chars_all} (~{total_chars_all*1.5:.0f} tokens)')

print('\n--- Sample recent posts (first 80 chars) ---')
rows = c.execute('SELECT time_text, LENGTH(full_text), substr(full_text, 1, 80) FROM posts ORDER BY time_text DESC LIMIT 15').fetchall()
for t, l, txt in rows:
    print(f'[{t}] ({l}ch) {txt}')

print('\n--- Sample older posts ---')
rows = c.execute('SELECT time_text, LENGTH(full_text), substr(full_text, 1, 80) FROM posts ORDER BY time_text ASC LIMIT 10').fetchall()
for t, l, txt in rows:
    print(f'[{t}] ({l}ch) {txt}')

print('\n--- Content type sampling (keywords) ---')
for kw, label in [('转债', '含"转债"'), ('下修', '含"下修"'), ('强赎', '含"强赎"'), ('买入', '含"买入"'), ('卖出', '含"卖出"'), ('持仓', '含"持仓"'), ('策略', '含"策略"'), ('估值', '含"估值"')]:
    cnt = c.execute(f"SELECT COUNT(*) FROM posts WHERE full_text LIKE '%{kw}%'").fetchone()[0]
    print(f'  {label}: {cnt}/{total} ({cnt*100//total}%)')
