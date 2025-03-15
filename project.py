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
    except Error as e:
        print("Fail")
        sys.exit(1)

##############################
# Function 1: Import Data
##############################
def import_data(folder):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        # Disable FK checks and drop tables
        cursor.execute("SET FOREIGN_KEY_CHECKS=0;")
        tables = ["Session", "Review", "Movie", "Video", "Viewer", "`Release`"]
        for t in tables:
            cursor.execute(f"DROP TABLE IF EXISTS {t};")
        conn.commit()
        # Create tables (use backticks for Release)
        create_release = """
            CREATE TABLE `Release` (
                rid INT PRIMARY KEY,
                genre VARCHAR(255),
                title VARCHAR(255)
            );
        """
        create_viewer = """
            CREATE TABLE Viewer (
                uid INT PRIMARY KEY,
                email VARCHAR(255),
                nickname VARCHAR(255),
                street VARCHAR(255),
                city VARCHAR(255),
                state VARCHAR(50),
                zip VARCHAR(20),
                genres VARCHAR(255),
                joined_date DATE,
                first VARCHAR(255),
                last VARCHAR(255),
                subscription VARCHAR(50)
            );
        """
        create_movie = """
            CREATE TABLE Movie (
                rid INT PRIMARY KEY,
                website_url VARCHAR(255),
                FOREIGN KEY (rid) REFERENCES `Release`(rid)
            );
        """
        create_session = """
            CREATE TABLE Session (
                sid INT PRIMARY KEY,
                uid INT,
                rid INT,
                ep_num INT,
                initiate_at DATETIME,
                leave_at DATETIME,
                quality VARCHAR(50),
                device VARCHAR(50),
                FOREIGN KEY (uid) REFERENCES Viewer(uid),
                FOREIGN KEY (rid) REFERENCES `Release`(rid)
            );
        """
        create_review = """
            CREATE TABLE Review (
                uid INT,
                rid INT,
                review TEXT,
                PRIMARY KEY (uid, rid),
                FOREIGN KEY (uid) REFERENCES Viewer(uid),
                FOREIGN KEY (rid) REFERENCES `Release`(rid)
            );
        """
        create_video = """
            CREATE TABLE Video (
                rid INT,
                ep_num INT,
                title VARCHAR(255),
                length INT,
                PRIMARY KEY (rid, ep_num),
                FOREIGN KEY (rid) REFERENCES `Release`(rid)
            );
        """
        for ddl in [create_release, create_viewer, create_movie, create_session, create_review, create_video]:
            cursor.execute(ddl)
        conn.commit()
        cursor.execute("SET FOREIGN_KEY_CHECKS=1;")
        conn.commit()

        # Helper: load CSV file
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

        load_csv("Release", 3)
        load_csv("Viewer", 12)
        load_csv("Movie", 2)
        load_csv("Session", 8)
        load_csv("Review", 3)
        load_csv("Video", 4)
        print("Success")
    except Exception as e:
        # Uncomment next line to debug: print(e)
        print("Fail")
    finally:
        cursor.close()
        conn.close()

##############################
# Function 2: Insert Viewer
##############################
def insert_viewer(params):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        query = """
            INSERT INTO Viewer 
            (uid, email, nickname, street, city, state, zip, genres, joined_date, first, last, subscription)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        cursor.execute(query, tuple(params))
        conn.commit()
        print("Success")
    except Exception as e:
        print("Fail")
    finally:
        cursor.close()
        conn.close()

##############################
# Function 3: Add Genre
##############################
def add_genre(uid, genre):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT genres FROM Viewer WHERE uid = %s;", (uid,))
        result = cursor.fetchone()
        if result is None:
            print("Fail")
            return
        current = result[0]
        if current is None or current.strip() == "":
            new_genres = genre
        else:
            current_list = [g.strip().lower() for g in current.split(';')]
            if genre.lower() in current_list:
                new_genres = current  # already exists
            else:
                new_genres = current + ";" + genre
        cursor.execute("UPDATE Viewer SET genres = %s WHERE uid = %s;", (new_genres, uid))
        conn.commit()
        print("Success")
    except Exception as e:
        print("Fail")
    finally:
        cursor.close()
        conn.close()

##############################
# Function 4: Delete Viewer
##############################
def delete_viewer(uid):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM Session WHERE uid = %s;", (uid,))
        cursor.execute("DELETE FROM Review WHERE uid = %s;", (uid,))
        cursor.execute("DELETE FROM Viewer WHERE uid = %s;", (uid,))
        conn.commit()
        print("Success")
    except Exception as e:
        print("Fail")
    finally:
        cursor.close()
        conn.close()

##############################
# Function 5: Insert Movie
##############################
def insert_movie(rid, website_url):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SET FOREIGN_KEY_CHECKS=0;")
        query = "INSERT INTO Movie (rid, website_url) VALUES (%s, %s);"
        cursor.execute(query, (rid, website_url))
        conn.commit()
        cursor.execute("SET FOREIGN_KEY_CHECKS=1;")
        print("Success")
    except Exception as e:
        print("Fail")
    finally:
        cursor.close()
        conn.close()

##############################
# Function 6: Insert Session
##############################
def insert_session(params):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SET FOREIGN_KEY_CHECKS=0;")
        query = """
            INSERT INTO Session (sid, uid, rid, ep_num, initiate_at, leave_at, quality, device)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """
        cursor.execute(query, tuple(params))
        conn.commit()
        cursor.execute("SET FOREIGN_KEY_CHECKS=1;")
        print("Success")
    except Exception as e:
        print("Fail")
    finally:
        cursor.close()
        conn.close()

##############################
# Function 7: Update Release
##############################
def update_release(rid, title):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        query = "UPDATE `Release` SET title = %s WHERE rid = %s;"
        cursor.execute(query, (title, rid))
        if cursor.rowcount == 0:
            cursor.execute("INSERT INTO `Release` (rid, genre, title) VALUES (%s, %s, %s);", (rid, "", title))
        conn.commit()
        print("Success")
    except Exception as e:
        print("Fail")
    finally:
        cursor.close()
        conn.close()

##############################
# Function 8: List Releases Reviewed by Viewer
##############################
def list_releases(uid):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        query = """
            SELECT DISTINCT r.rid, r.genre, r.title 
            FROM Review rv JOIN `Release` r ON rv.rid = r.rid 
            WHERE rv.uid = %s 
            ORDER BY r.title ASC;
        """
        cursor.execute(query, (uid,))
        rows = cursor.fetchall()
        for row in rows:
            print(",".join(str(x) for x in row))
    except Exception as e:
        print("Fail")
    finally:
        cursor.close()
        conn.close()

##############################
# Function 9: Popular Release
##############################
def popular_release(N):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        query = """
            SELECT r.rid, r.title, COUNT(rv.uid) AS reviewCount
            FROM `Release` r 
            LEFT JOIN Review rv ON r.rid = rv.rid
            GROUP BY r.rid, r.title
            ORDER BY reviewCount DESC, r.rid ASC
            LIMIT %s;
        """
        cursor.execute(query, (N,))
        rows = cursor.fetchall()
        for row in rows:
            print(",".join(str(x) for x in row))
    except Exception as e:
        print("Fail")
    finally:
        cursor.close()
        conn.close()

##############################
# Function 10: Title of Release by Session ID
##############################
def release_title(sid):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        query = """
            SELECT r.rid, r.title, r.genre, v.title, s.ep_num, v.length
            FROM Session s 
            JOIN `Release` r ON s.rid = r.rid
            JOIN Video v ON s.rid = v.rid AND s.ep_num = v.ep_num
            WHERE s.sid = %s;
        """
        cursor.execute(query, (sid,))
        rows = cursor.fetchall()
        for row in rows:
            print(",".join("" if x is None else str(x) for x in row))
    except Exception as e:
        print("Fail")
    finally:
        cursor.close()
        conn.close()

##############################
# Function 11: Active Viewers
##############################
def active_viewer(N, start_date, end_date):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        query = """
            SELECT v.uid, v.first, v.last
            FROM Viewer v
            WHERE v.uid IN (
                SELECT s.uid
                FROM Session s
                WHERE s.initiate_at BETWEEN CONCAT(%s, ' 00:00:00') AND CONCAT(%s, ' 23:59:59')
                GROUP BY s.uid
                HAVING COUNT(s.sid) >= %s
            )
            ORDER BY v.uid ASC;
        """
        cursor.execute(query, (start_date, end_date, N))
        rows = cursor.fetchall()
        for row in rows:
            print(",".join(str(x) for x in row))
    except Exception as e:
        print("Fail")
    finally:
        cursor.close()
        conn.close()

##############################
# Function 12: Number of Videos Viewed
##############################
def videos_viewed(rid):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        query = """
            SELECT v.rid, v.ep_num, v.title, v.length, COUNT(DISTINCT s.uid) AS viewerCount
            FROM Video v 
            LEFT JOIN Session s ON v.rid = s.rid AND v.ep_num = s.ep_num
            WHERE v.rid = %s 
            GROUP BY v.rid, v.ep_num, v.title, v.length
            ORDER BY v.ep_num ASC;
        """
        cursor.execute(query, (rid,))
        rows = cursor.fetchall()
        for row in rows:
            print(",".join(str(x) for x in row))
    except Exception as e:
        print("Fail")
    finally:
        cursor.close()
        conn.close()

##############################
# Main Dispatch Function
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
            params = [
                uid,
                sys.argv[3],
                sys.argv[4],
                sys.argv[5],
                sys.argv[6],
                sys.argv[7],
                sys.argv[8],
                sys.argv[9],
                sys.argv[10],
                sys.argv[11],
                sys.argv[12],
                sys.argv[13]
            ]
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
