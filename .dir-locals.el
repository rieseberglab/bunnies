; -*- mode: Lisp -*-

(
 (python-mode
  . (
     ;; This is used to fill the paragraph in 120 columns when pressing M-q
     (fill-column . 120)

     ;; Use 4 spaces to indent in Python
     (python-indent-offset . 4)

     ;; docformatter global options
     (py-docformatter-options . ("--wrap-summaries=120" "--wrap-descriptions=120" "--pre-summary-newline" "--no-blank" "--make-summary-multi-line"))

     ;; autoflake global options
     ;;(py-autoflake-options . ("--remove-all-unused-imports" "--remove-unused-variables"))

     (flycheck-flake8rc . ".flake8"))))

