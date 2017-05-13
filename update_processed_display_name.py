from nyaa import app, db, models

import time

def row_id_iterator(class_id, query, limit=1000, last_id=None):
    while True:
        ended = True
        new_filter = False

        if last_id is not None:
            q = query.filter(class_id > last_id)
            new_filter = True
        else:
            q = query
        
        limit_q = q.limit(limit)
        limit_q_size = limit_q.count()
        for i, row in enumerate(limit_q):
            ended = False
            last_id = row.id
            yield row, i == limit_q_size - 1
        
        if ended:
            break

session = db.session

chunk_size = 20


total_torrents = models.Torrent.query.count()
pad_size = len(str(total_torrents))
print("Updating", total_torrents, "torrents...")

chunked_iterator = row_id_iterator(models.Torrent.id, models.Torrent.query, limit=chunk_size)

next_time = time.time() + 1
for torrent, new_chunk_next in chunked_iterator:
    torrent.update_processed_display_name(None, torrent.display_name)
    
    if new_chunk_next:
        session.flush()
    
    now = time.time()
    if now > next_time:
        perc = (torrent.id / total_torrents) * 100
        print("At ID #{}/{} - {:03.1f}".format(str(torrent.id).zfill(pad_size), total_torrents, perc))
        next_time = now + 10

print("Last commit...")
session.commit()
print("Updated processed_display_name on", total_torrents, "torrents.")