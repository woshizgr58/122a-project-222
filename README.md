# 🎬 ZotStreaming – Streaming Platform Manager

A MySQL-backed database management system for a streaming service, built in Python.
The project provides a command-line interface (CLI) to import, insert, update, and query streaming-related data such as users, viewers, releases, movies, sessions, reviews, and videos.

⸻

## 🚀 Features
```
	•	Database Schema Initialization
	•	Automatically drops and recreates all required tables (Users, Viewers, Releases, Movies, Sessions, Reviews, Videos)
	•	Bulk imports CSV data for each table
	•	User & Viewer Management
	•	Insert new viewers (insertViewer)
	•	Add new genres to a user’s profile (addGenre)
	•	Delete a viewer and cascade clean-up of related sessions and reviews (deleteViewer)
	•	Content Management
	•	Insert new movies (insertMovie)
	•	Insert new sessions (insertSession)
	•	Update or add releases (updateRelease)
	•	Queries & Reports
	•	List all releases reviewed by a specific viewer (listReleases)
	•	Find the most popular releases ranked by review count (popularRelease)
	•	Get release details from a session ID (releaseTitle)
	•	Identify active viewers in a date range (activeViewer)
	•	Count number of viewers per video (videosViewed)
```
⸻

## 📂 Database Schema
```
The project creates and manages the following tables:
	•	Users – stores account details, address, and preferred genres
	•	Viewers – subscription tier and viewer’s personal info (1:1 with Users)
	•	Releases – generic release info (title, genre)
	•	Movies – movie-specific data linked to Releases
	•	Videos – episodes with metadata (title, length)
```
