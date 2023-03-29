;;; websocket-bridge-database.el --- Websocket-bridge plugin for Database  -*- lexical-binding: t; -*-

;; Copyright (C) 2022  Qiqi Jin

;; Author: Qiqi Jin <ginqi7@gmail.com>
;; Keywords: lisp

;; This program is free software; you can redistribute it and/or modify
;; it under the terms of the GNU General Public License as published by
;; the Free Software Foundation, either version 3 of the License, or
;; (at your option) any later version.

;; This program is distributed in the hope that it will be useful,
;; but WITHOUT ANY WARRANTY; without even the implied warranty of
;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;; GNU General Public License for more details.

;; You should have received a copy of the GNU General Public License
;; along with this program.  If not, see <https://www.gnu.org/licenses/>.

;;; Commentary:

;;

;;; Code:

(require 'ctable)
(require 'websocket-bridge)
(require 'ht)

(defgroup websocket-bridge-database
  ()
  "Websocket-bridge plugin for database."
  :group 'applications)

(defcustom websocket-bridge-database-user-data-directory
  (locate-user-emacs-file "websocket-bridge-database-data/")
  "Place user data in Emacs directory."
  :group 'websocket-bridge-database
  :type '(directory))

(defvar websocket-bridge-database-py-path
  (concat
   (file-name-directory load-file-name)
   "websocket_bridge_database.py"))

(defvar websocket-bridge-database-db-metas nil)

(defvar websocket-bridge-database-db-databases nil)

(defvar websocket-bridge-database-db-selected-items "*")

(defvar websocket-bridge-database-db-limit "10")


(defun websocket-bridge-database-start ()
  "Start websocket bridge database."
  (interactive)
  (websocket-bridge-app-start
   "database"
   "python3"
   websocket-bridge-database-py-path))

(defun websocket-bridge-database-get-db-meta (name)
  "Get current db meta by NAME."
  (websocket-bridge-call "database" "get_db_meta" name))

(defun websocket-bridge-database-refresh-data(db-name database sql)
  "Query Database refresh data with DB-NAME DATABASE SQL."
  (websocket-bridge-run-sql db-name database sql))

(defun websocket-bridge-database ()
  "Websocket bridge database entry."
  (interactive)
  (if websocket-bridge-database-db-metas
      (progn
        (websocket-bridge-database-open
         (completing-read "Choose database: "
                          (hash-table-keys websocket-bridge-database-db-metas)
                          nil t)))
    (when (yes-or-no-p "There is no database.  Do you want to create one?")
      (websocket-bridge-database-new))))

(defun websocket-bridge-database-new ()
  "Create a database."
  (interactive)
  (let ((name (read-minibuffer "Please Input database name: "))
        (db-type
         (completing-read "Please Select a database type: "
                          '("MySQL" "PostgreSQL")))
        (host (read-minibuffer "Please Input database host: "))
        (port (read-minibuffer "Please Input database port: "))
        (username
         (read-minibuffer "Please Input database username: "))
        (password (password-read "Please Input database password: ")))
    (websocket-bridge-call "database" "new_database" name db-type host port username password)))

(defun websocket-bridge-database-open (name)
  "Open `websocket-bridge-database' buffer by NAME."
  (setq websocket-bridge-database-data-component nil)
  (setq websocket-bridge-database-meta-component nil)
  (with-current-buffer (get-buffer-create "*websocket-bridge-database-meta*")
    (setq buffer-read-only nil)
    (erase-buffer)
    (setq websocket-bridge-database-db-name name)
    (let ((meta
           (gethash (intern name) websocket-bridge-database-db-metas)))
      (when meta
        (setq meta (ht<-plist meta))
        (maphash
         (lambda (key value)
           (set
            (intern (format "websocket-bridge-database-db-%s" key))
            value))
         meta)))
    (websocket-bridge-database-show-databases name)
    (websocket-bridge-database-meta)
    (goto-char (point-max))
    (setq buffer-read-only t))
  (switch-to-buffer "*websocket-bridge-database-meta*"))

(defun websocket-bridge-database-show-databases (db-name)
  "Call python show databases about DB-NAME."
  (websocket-bridge-call "database" "show_databases" db-name))


(defun websocket-bridge-database-stop ()
  "Stop websocket bridge database."
  (interactive)
  (websocket-bridge-app-exit "database"))

(defun websocket-bridge-database-restart ()
  "Restart websocket bridge database."
  (interactive)
  (websocket-bridge-database-stop)
  (websocket-bridge-database-start))

(defun websocket-bridge-run-sql (db-name database sql)
  "Call python function to run SQL with DB-NAME DATABASE."
  (websocket-bridge-call "database" "run_sql" db-name database sql))


(defun websocket-bridge-database-select-candidates (type candidates)
  "Select CANDIDATES and set value to variable bind TYPE."
  (set
   (intern (format "websocket-bridge-database-db-%s" type))
   (string-join (completing-read-multiple (format "Select %s: " type) candidates nil t) ",")))

(defun websocket-bridge-database-build-sql()
  "Build sql."
  (let ((selected-items websocket-bridge-database-db-selected-items)
        (table (ignore-errors websocket-bridge-database-db-table))
        (where (ignore-errors websocket-bridge-database-db-where))
        (order (ignore-errors websocket-bridge-database-db-order))
        (limit (ignore-errors websocket-bridge-database-db-limit))
        sql)
    (if (and selected-items table)
        (progn
          (setq sql
                (format "SELECT %s FROM %s" selected-items table))
          (when where (setq sql (format "%s WHERE %s" sql where)))
          (when order (setq sql (format "%s ORDER BY %s" sql order)))
          (when limit (setq sql (format "%s Limit %s" sql limit)))
          (setq websocket-bridge-database-db-sql sql))
      (print "Selected items and table must required."))))

(defun websocket-bridge-database-read-from-minibuffer (prompt)
  "Read from minibuffer, change empty string to nil.
PROMPT is `read-string' prompt."
  (let ((value (read-string prompt)))
    (if (string= value "") nil value)))

(defun websocket-bridge-database-update-meta (type)
  "Update database meta view.
TYPE is database meta type"
  (pcase type
    ("name"
     (websocket-bridge-database-select-candidates type
                                                  (hash-table-keys websocket-bridge-database-db-metas)))
    ("database"
     (websocket-bridge-database-select-candidates type websocket-bridge-database-db-databases)
     (websocket-bridge-database-show-tables websocket-bridge-database-db-name websocket-bridge-database-db-database)
     (ignore-errors
       (set websocket-bridge-database-db-table nil)
       (set websocket-bridge-database-db-selected-items "*")
       (set websocket-bridge-database-db-where nil)
       (set websocket-bridge-database-db-order nil)
       (set websocket-bridge-database-db-limit "10")))
    ("table"
     (websocket-bridge-database-select-candidates type websocket-bridge-database-db-tables))
    ("selected-items"
     (websocket-bridge-database-select-candidates type websocket-bridge-database-db-columns)
     ;; (setq websocket-bridge-database-db-selected-items
     ;;       (websocket-bridge-database-read-from-minibuffer "Please input where conditions: "))
     )
    ("where"
     (setq websocket-bridge-database-db-where
           (websocket-bridge-database-read-from-minibuffer "Please input where conditions: "))
     (when (string= websocket-bridge-database-db-where "")
       (setq websocket-bridge-database-db-where nil)))
    ("order"
     (setq websocket-bridge-database-db-order
           (websocket-bridge-database-read-from-minibuffer "Please input Order By: ")))
    ("limit"
     (setq websocket-bridge-database-db-limit
           (websocket-bridge-database-read-from-minibuffer "Please input limit: "))))

  (save-excursion
    (websocket-bridge-database-build-sql)
    (websocket-bridge-database-meta)
    (when (ignore-errors websocket-bridge-database-db-sql)
      (websocket-bridge-database-refresh-data
       websocket-bridge-database-db-name
       websocket-bridge-database-db-database
       websocket-bridge-database-db-sql))))

(defun websocket-bridge-database-show-tables (db-name database)
  "Show tables in DB-NAME and DATABASE."
  (websocket-bridge-call "database" "show_tables" db-name database))

(defun websocket-bridge-database-show-columns (db-name database table-name)
  "Show columns in DB-NAME and DATABASE and TABLE-NAME."
  (websocket-bridge-call "database" "show_columns" db-name database table-name))

(defun websocket-bridge-database-buttonize (type)
  "Create a buttonize string with TYPE."
  (let* ((value
          (ignore-errors
            (symbol-value
             (intern (format "websocket-bridge-database-db-%s" type)))))
         (value (if value value "nil")))
    (buttonize
     value
     (lambda (_button) (websocket-bridge-database-update-meta type)))))

(defun websocket-bridge-database-meta ()
  "Create ctable about database meta."
  (let* ((column-names '("meta-name" "value"))
         (param (copy-ctbl:param ctbl:default-rendering-param))
         (data
          (list
           (list "Name" (websocket-bridge-database-buttonize "name"))
           (list "Database Type" websocket-bridge-database-db-db_type)
           (list "Database Host" websocket-bridge-database-db-host)
           (list "Database Port" websocket-bridge-database-db-port)
           (list "Database Name"
                 (websocket-bridge-database-buttonize "database"))
           (list "Database Table"
                 (websocket-bridge-database-buttonize "table"))
           (list "Database Select Items"
                 (websocket-bridge-database-buttonize "selected-items"))
           (list "Database Where Conditions"
                 (websocket-bridge-database-buttonize "where"))
           (list "Database Order By"
                 (websocket-bridge-database-buttonize "order"))
           (list "Database Limit"
                 (websocket-bridge-database-buttonize "limit"))
           (list "SQL" (websocket-bridge-database-buttonize "sql"))))
         (column-model
          (mapcar
           (lambda (name)
             (make-ctbl:cmodel :title name :min-width 5 :align 'left))
           column-names))
         (model
          (make-ctbl:model :column-model column-model :data data)))
    (save-excursion
      (if websocket-bridge-database-meta-component
          (ctbl:cp-set-model websocket-bridge-database-meta-component model)
        (setq websocket-bridge-database-meta-component
              (with-current-buffer "*websocket-bridge-database-meta*"
                (setq buffer-read-only nil)
                (goto-char (point-max))
                (ctbl:create-table-component-region :model model :param param)))))))

(defun websocket-bridge-database-show (column-names data)
  "Show database result : COLUMN-NAMES and DATA by ctable."
  (let* ((column-model
          (mapcar
           (lambda (name)
             (make-ctbl:cmodel :title name :min-width 5 :align 'left))
           column-names))
         (model
          (make-ctbl:model :column-model column-model :data data)))
    (save-excursion
      (if websocket-bridge-database-data-component
          (ctbl:cp-set-model websocket-bridge-database-data-component model)
        (setq websocket-bridge-database-data-component
              (with-current-buffer (get-buffer-create "*websocket-bridge-database-data*")
                (setq buffer-read-only nil)
                (goto-char (point-max))
                (ctbl:create-table-component-region :model model))))
      (pop-to-buffer "*websocket-bridge-database-data*"))))

(provide 'websocket-bridge-database)
;;; websocket-bridge-database.el ends here
