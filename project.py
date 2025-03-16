#!/usr/bin/env python3
import sys
import os
import csv
import mysql.connector
from mysql.connector import Error

##############################
# Helper: Database Connection
##############################
def get_connection():
    try:
        conn = mysql.connector.connect(user='test', password='password', database='cs122a')
        return conn
    except Error:
        print("Fail")
        sys.exit(1)

##############################
# 1) Import Data
##############################
def import_data(folder):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SET FOREIGN_KEY_CHECKS=0;")
        # Drop tables in proper order.
        tables = ["Sessions", "Reviews", "Movies", "Videos", "Viewers", "Users", "`Releases`"]
        for t in tables:
            cursor.execute(f"DROP TABLE IF EXISTS {t};")
        conn.commit()

        # Create Users table (from HW2)
        create_users = """
            CREATE TABLE Users (
                uid INT PRIMARY KEY,
                email TEXT NOT NULL,
                joined_date DATE NOT NULL,
                nickname TEXT NOT NULL,
                street TEXT,
                city TEXT,
                state TEXT,
                zip TEXT,
                genres TEXT
            );
        """
        # Create Viewers table (delta table for ISA)
        create_viewers = """
            CREATE TABLE Viewers (
                uid INT PRIMARY KEY,
                subscription ENUM('free','monthly','yearly'),
                first TEXT NOT NULL,
                last TEXT NOT NULL,
                FOREIGN KEY (uid) REFERENCES Users(uid) ON DELETE CASCADE
            );
        """
        # Remaining tables per HW2 solution or your design.
        create_releases = """
            CREATE TABLE Releases (
                rid INT PRIMARY KEY,
                genre VARCHAR(255),
                title VARCHAR(255)
            );
        """
        create_movies = """
            CREATE TABLE Movies (
                rid INT PRIMARY KEY,
                website_url VARCHAR(255),
                FOREIGN KEY (rid) REFERENCES Releases(rid)
            );
        """
        create_sessions = """
            CREATE TABLE Sessions (
                sid INT PRIMARY KEY,
                uid INT,
                rid INT,
                ep_num INT,
                initiate_at DATETIME,
                leave_at DATETIME,
                quality VARCHAR(50),
                device VARCHAR(50),
                FOREIGN KEY (uid) REFERENCES Viewers(uid),
                FOREIGN KEY (rid) REFERENCES Releases(rid)
            );
        """
        create_reviews = """
            CREATE TABLE Reviews (
                uid INT,
                rid INT,
                review TEXT,
                PRIMARY KEY (uid, rid),
                FOREIGN KEY (uid) REFERENCES Viewers(uid),
                FOREIGN KEY (rid) REFERENCES Releases(rid)
            );
        """
        create_videos = """
            CREATE TABLE Videos (
                rid INT,
                ep_num INT,
                title VARCHAR(255),
                length INT,
                PRIMARY KEY (rid, ep_num),
                FOREIGN KEY (rid) REFERENCES Releases(rid)
            );
        """
        ddl_statements = [create_users, create_viewers, create_releases, create_movies, create_sessions, create_reviews, create_videos]
        for ddl in ddl_statements:
            cursor.execute(ddl)
        conn.commit()

        cursor.execute("SET FOREIGN_KEY_CHECKS=1;")
        conn.commit()

        def load_csv(table_name, num_cols):
            file_path = os.path.join(folder, f"{table_name}.csv")
            if not os.path.isfile(file_path):
                return
            with open(file_path, newline='', encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile)
                rows = list(reader)
                if not rows:
                    return
                placeholders = ",".join(["%s"] * num_cols)
                query = f"INSERT INTO {table_name} VALUES ({placeholders});"
                for row in rows:
                    row = [col if col != "" else None for col in row]
                    cursor.execute(query, row)
                conn.commit()

        # Load CSV files; filenames should match table names.
        load_csv("Releases", 3)
        load_csv("Users", 9)      # uid, email, joined_date, nickname, street, city, state, zip, genres
        load_csv("Viewers", 4)    # uid, subscription, first, last
        load_csv("Movies", 2)
        load_csv("Sessions", 8)
        load_csv("Reviews", 3)
        load_csv("Videos", 4)

        print("Success")
    except Exception:
        print("Fail")
    finally:
        cursor.close()
        conn.close()

##############################
# 2) Insert Viewer
##############################
def insert_viewer(params):
    # Expected order: uid, email, nickname, street, city, state, zip, genres, joined_date, first, last, subscription
    conn = get_connection()
    cursor = conn.cursor()
    try:
        query_users = """
            INSERT INTO Users (uid, email, joined_date, nickname, street, city, state, zip, genres)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        uid = params[0]
        email = params[1]
        nickname = params[2]
        street = params[3]
        city = params[4]
        state = params[5]
        zip_code = params[6]
        genres = params[7]
        joined_date = params[8]
        first = params[9]
        last = params[10]
        subscription = params[11]
        cursor.execute(query_users, (uid, email, joined_date, nickname, street, city, state, zip_code, genres))
        conn.commit()

        query_viewers = """
            INSERT INTO Viewers (uid, subscription, first, last)
            VALUES (%s, %s, %s, %s);
        """
        cursor.execute(query_viewers, (uid, subscription, first, last))
        conn.commit()
        print("Success")
    except Exception:
        print("Fail")
    finally:
        cursor.close()
        conn.close()

##############################
# 3) Add Genre
##############################
def add_genre(uid, genre):
    # For addGenre, per HW2, genres is stored in Users.genres as a comma-separated list.
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT genres FROM Users WHERE uid = %s;", (uid,))
        result = cursor.fetchone()
        # If no viewer found, print "Success" (per discussion)
        if result is None:
            print("Success")
            return
        current = result[0] if result[0] is not None else ""
        current = current.strip()
        new_genre = genre.strip()
        if current == "":
            new_genres = new_genre
        else:
            # Split on commas since HW2 uses commas.
            current_list = [g.strip().lower() for g in current.split(',')]
            if new_genre.lower() in current_list:
                print("Fail")
                return
            else:
                new_genres = current + "," + new_genre
        cursor.execute("UPDATE Users SET genres = %s WHERE uid = %s;", (new_genres, uid))
        conn.commit()
        print("Success")
    except Exception:
        print("Fail")
    finally:
        cursor.close()
        conn.close()

##############################
# 4) Delete Viewer
##############################
def delete_viewer(uid):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM Sessions WHERE uid = %s;", (uid,))
        cursor.execute("DELETE FROM Reviews WHERE uid = %s;", (uid,))
        cursor.execute("DELETE FROM Viewers WHERE uid = %s;", (uid,))
        conn.commit()
        print("Success")
    except Exception:
        print("Fail")
    finally:
        cursor.close()
        conn.close()

##############################
# 5) Insert Movie
##############################
def insert_movie(rid, website_url):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SET FOREIGN_KEY_CHECKS=0;")
        query = "INSERT INTO Movies (rid, website_url) VALUES (%s, %s);"
        cursor.execute(query, (rid, website_url))
        conn.commit()
        cursor.execute("SET FOREIGN_KEY_CHECKS=1;")
        print("Success")
    except Exception:
        print("Fail")
    finally:
        cursor.close()
        conn.close()

##############################
# 6) Insert Session
##############################
def insert_session(params):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SET FOREIGN_KEY_CHECKS=0;")
        query = """
            INSERT INTO Sessions
            (sid, uid, rid, ep_num, initiate_at, leave_at, quality, device)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """
        cursor.execute(query, tuple(params))
        conn.commit()
        cursor.execute("SET FOREIGN_KEY_CHECKS=1;")
        print("Success")
    except Exception:
        print("Fail")
    finally:
        cursor.close()
        conn.close()

##############################
# 7) Update Release
##############################
def update_release(rid, title):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        query = "UPDATE `Releases` SET title = %s WHERE rid = %s;"
        cursor.execute(query, (title, rid))
        if cursor.rowcount == 0:
            cursor.execute("INSERT INTO `Releases` (rid, genre, title) VALUES (%s, %s, %s);", (rid, "", title))
        conn.commit()
        print("Success")
    except Exception:
        print("Fail")
    finally:
        cursor.close()
        conn.close()

##############################
# 8) List Releases Reviewed by Viewer
##############################
def list_releases(uid):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        query = """
            SELECT DISTINCT r.rid, r.genre, r.title
            FROM Reviews rv
            JOIN `Releases` r ON rv.rid = r.rid
            WHERE rv.uid = %s
            ORDER BY r.title ASC;
        """
        cursor.execute(query, (uid,))
        rows = cursor.fetchall()
        if not rows:
            print("Fail")
        else:
            for row in rows:
                print(",".join(str(x) for x in row))
    except Exception:
        print("Fail")
    finally:
        cursor.close()
        conn.close()

##############################
# 9) Popular Release
##############################
def popular_release(N):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        query = """
            SELECT r.rid, r.title, COUNT(rv.uid) AS reviewCount
            FROM `Releases` r
            LEFT JOIN Reviews rv ON r.rid = rv.rid
            GROUP BY r.rid, r.title
            ORDER BY reviewCount DESC, r.rid DESC
            LIMIT %s;
        """
        cursor.execute(query, (N,))
        rows = cursor.fetchall()
        if not rows:
            print("Fail")
        else:
            for row in rows:
                print(",".join(str(x) for x in row))
    except Exception:
        print("Fail")
    finally:
        cursor.close()
        conn.close()

##############################
# 10) Title of Release by Session ID
##############################
def release_title(sid):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        query = """
            SELECT r.rid, r.title, r.genre, IFNULL(v.title,''), s.ep_num, IFNULL(v.length,0)
            FROM Sessions s
            JOIN `Releases` r ON s.rid = r.rid
            LEFT JOIN Videos v ON s.rid = v.rid AND s.ep_num = v.ep_num
            WHERE s.sid = %s
            ORDER BY r.title ASC;
        """
        cursor.execute(query, (sid,))
        rows = cursor.fetchall()
        if not rows:
            print("Fail")
        else:
            for row in rows:
                print(",".join("" if x is None else str(x) for x in row))
    except Exception:
        print("Fail")
    finally:
        cursor.close()
        conn.close()

##############################
# 11) Active Viewers
##############################
def active_viewer(N, start_date, end_date):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        # Compare full datetime strings (no DATE() wrapper)
        query = """
            SELECT v.uid, v.first, v.last
            FROM Viewers v
            JOIN Sessions s ON v.uid = s.uid
            WHERE s.initiate_at BETWEEN %s AND %s
            GROUP BY v.uid, v.first, v.last
            HAVING COUNT(s.sid) >= %s
            ORDER BY v.uid ASC;
        """
        cursor.execute(query, (start_date, end_date, N))
        rows = cursor.fetchall()
        if not rows:
            print("Fail")
        else:
            for row in rows:
                print(",".join(str(x) for x in row))
    except Exception:
        print("Fail")
    finally:
        cursor.close()
        conn.close()

##############################
# 12) Number of Videos Viewed
##############################
def videos_viewed(rid):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        query = """
            SELECT v.rid, v.ep_num, v.title, v.length,
                   COUNT(DISTINCT s.uid) AS viewerCount
            FROM Videos v
            LEFT JOIN Sessions s
              ON v.rid = s.rid AND v.ep_num = s.ep_num
            WHERE v.rid = %s
            GROUP BY v.rid, v.ep_num, v.title, v.length
            ORDER BY v.rid DESC, v.ep_num ASC;
        """
        cursor.execute(query, (rid,))
        rows = cursor.fetchall()
        if not rows:
            print("Fail")
        else:
            for row in rows:
                print(",".join(str(x) for x in row))
    except Exception:
        print("Fail")
    finally:
        cursor.close()
        conn.close()

##############################
# Main Dispatch
##############################
def main():
    if len(sys.argv) < 2:
        print("Fail")
        return

    func = sys.argv[1]
    if func == "import":
        if len(sys.argv) != 3:
            print("Fail")
            return
        import_data(sys.argv[2])
    elif func == "insertViewer":
        if len(sys.argv) != 14:
            print("Fail")
            return
        try:
            uid = int(sys.argv[2])
            params = [uid, sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6],
                      sys.argv[7], sys.argv[8], sys.argv[9], sys.argv[10],
                      sys.argv[11], sys.argv[12], sys.argv[13]]
        except:
            print("Fail")
            return
        insert_viewer(params)
    elif func == "addGenre":
        if len(sys.argv) != 4:
            print("Fail")
            return
        try:
            uid = int(sys.argv[2])
            genre = sys.argv[3]
        except:
            print("Fail")
            return
        add_genre(uid, genre)
    elif func == "deleteViewer":
        if len(sys.argv) != 3:
            print("Fail")
            return
        try:
            uid = int(sys.argv[2])
        except:
            print("Fail")
            return
        delete_viewer(uid)
    elif func == "insertMovie":
        if len(sys.argv) != 4:
            print("Fail")
            return
        try:
            rid = int(sys.argv[2])
            website_url = sys.argv[3]
        except:
            print("Fail")
            return
        insert_movie(rid, website_url)
    elif func == "insertSession":
        if len(sys.argv) != 10:
            print("Fail")
            return
        try:
            sid = int(sys.argv[2])
            uid = int(sys.argv[3])
            rid = int(sys.argv[4])
            ep_num = int(sys.argv[5])
            initiate_at = sys.argv[6]
            leave_at = sys.argv[7]
            quality = sys.argv[8]
            device = sys.argv[9]
            params = [sid, uid, rid, ep_num, initiate_at, leave_at, quality, device]
        except:
            print("Fail")
            return
        insert_session(params)
    elif func == "updateRelease":
        if len(sys.argv) != 4:
            print("Fail")
            return
        try:
            rid = int(sys.argv[2])
            title = sys.argv[3]
        except:
            print("Fail")
            return
        update_release(rid, title)
    elif func == "listReleases":
        if len(sys.argv) != 3:
            print("Fail")
            return
        try:
            uid = int(sys.argv[2])
        except:
            print("Fail")
            return
        list_releases(uid)
    elif func == "popularRelease":
        if len(sys.argv) != 3:
            print("Fail")
            return
        try:
            N = int(sys.argv[2])
        except:
            print("Fail")
            return
        popular_release(N)
    elif func == "releaseTitle":
        if len(sys.argv) != 3:
            print("Fail")
            return
        try:
            sid = int(sys.argv[2])
        except:
            print("Fail")
            return
        release_title(sid)
    elif func == "activeViewer":
        if len(sys.argv) != 5:
            print("Fail")
            return
        try:
            N = int(sys.argv[2])
            start_date = sys.argv[3]
            end_date = sys.argv[4]
        except:
            print("Fail")
            return
        active_viewer(N, start_date, end_date)
    elif func == "videosViewed":
        if len(sys.argv) != 3:
            print("Fail")
            return
        try:
            rid = int(sys.argv[2])
        except:
            print("Fail")
            return
        videos_viewed(rid)
    else:
        print("Fail")

if __name__ == "__main__":
    main()