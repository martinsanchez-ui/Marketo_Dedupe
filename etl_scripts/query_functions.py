"""
query_functions.py    Mar/2025    Jose Rosati
    Tests       :
    Description : Query functions for Marketo Dedupe
"""

import sqlite3

from global_settings import bi_db, log, mask_email


def ensure_unique_record(contact_id, marketo_id):
    """ Ensure that there is an unique record in email_marketing_f with a given contact_id and marketo_id """
    
    SELECT_QUERY =  f""" 
                        SELECT
	                        COUNT(DISTINCT email_marketing_f_id)
                        FROM
                            bi_warehouse_prd.email_marketing_f 
                        WHERE 
                            contact_id = {contact_id} 
                            AND row_status = 'Active' 
                            AND marketo_id = {marketo_id};
                    """

    cursor = bi_db.cursor()
    cursor.execute(SELECT_QUERY)
    number_of_records = cursor.fetchone()[0]
    cursor.close()
    
    if number_of_records != 1:
        log.error(f"[MKTO_DEDUPE] step=ensure_unique_record action=error contact_id={contact_id} "
                  f"marketo_id={marketo_id} records_found={number_of_records}")
        return False
    
    return True

def double_check_not_found_in_marketo(sqlite_conn, marketo_id):
    """ Used to check if a marketo_id is present in the dump or not.
        This function is used as a complementary check to determine if a record is exclusively found in the CRM and not in Marketo.
        This avoids false positives in detecting CRM only records when the email address is different between the CRM and Marketo. 

        This function uses the sqlite_conn already opened in the calling function because it runs multiple times
    """

    GET_MKTO_ONLY = """
        SELECT COUNT(Id)
        FROM mkto_dump
        WHERE Id = %s;
    """

    cursor = sqlite_conn.cursor()
    result = cursor.execute(GET_MKTO_ONLY % marketo_id).fetchone()
    return not result[0]

def get_contact_type_categories():
    """ Gets a dictionary of Category Types IDs and References """

    GET_CATEGORY_TYPES_QUERY = """
        SELECT 
            DISTINCT contact_type_category_id, 
            contact_type_category

        FROM 
            bi_warehouse_prd.contact_type_d
        
        ORDER BY 
            contact_type_category_id;
    """

    bi_db_cursor = bi_db.cursor()
    bi_db_cursor.execute(GET_CATEGORY_TYPES_QUERY)
    category_types = bi_db_cursor.fetchall()
    bi_db_cursor.close()

    return dict(category_types)

def get_records_from_email_marketing_f(contact_id, email):
    """ Gets the records present in email_marketing_f for a given contact_id """

    GET_RECORDS_EMAIL_MARKETING_F = """
        SELECT 
            emf.marketo_id, 
            CAST(emf.contact_id AS CHAR) AS contact_id, 
            cd.full_name, 
            CASE 
                WHEN cc.custom2 > 0 THEN cd.marketing_email 
                WHEN cc.custom2 = 0 AND IF(emf.submitted_email = '', cd.marketing_email, emf.submitted_email) <> 'Unknown' THEN IF(emf.submitted_email = '', cd.marketing_email, emf.submitted_email) 
                WHEN cd.email_address <> 'Unknown' THEN cd.email_address 
                WHEN cd.email_address2 <> 'Unknown' THEN cd.email_address2 
                WHEN cd.email_address3 <> 'Unknown' THEN cd.email_address3 
                WHEN cd.email_address4 <> 'Unknown' THEN cd.email_address4 
            END AS email, 
            cd.contact_type_category 
        FROM 
            bi_warehouse_prd.email_marketing_f emf 
            JOIN bi_warehouse_prd.contact_d cd ON (emf.contact_id = cd.contact_id) 
            JOIN xrms.contacts cc ON (emf.contact_id = cc.contact_id) 
        WHERE 
            emf.contact_id = %s 
            AND emf.row_status = 'Active' 
            AND (
                CASE 
                    WHEN cc.custom2 > 0 THEN cd.marketing_email 
                    WHEN cc.custom2 = 0 AND IF(emf.submitted_email = '', cd.marketing_email, emf.submitted_email) <> 'Unknown' THEN IF(emf.submitted_email = '', cd.marketing_email, emf.submitted_email) 
                    WHEN cd.email_address <> 'Unknown' THEN cd.email_address 
                    WHEN cd.email_address2 <> 'Unknown' THEN cd.email_address2 
                    WHEN cd.email_address3 <> 'Unknown' THEN cd.email_address3 
                    WHEN cd.email_address4 <> 'Unknown' THEN cd.email_address4 
                END
            ) = '%s';
    """

    log.debug(f"[MKTO_DEDUPE] step=get_records_from_email_marketing_f action=start contact_id={contact_id} "
              f"email={mask_email(email)}")
    bi_db_cursor = bi_db.cursor()
    bi_db_cursor.execute(GET_RECORDS_EMAIL_MARKETING_F % (contact_id, email))
    records = bi_db_cursor.fetchall()
    bi_db_cursor.close()

    log.debug(f"[MKTO_DEDUPE] step=get_records_from_email_marketing_f action=complete contact_id={contact_id} "
              f"records_found={len(records)}")
    return records

def is_deleted_contact(contact_id):
    """ Checks if the contact_id is marked as deleted in the CRM """

    IS_DELETED_SQL = """
        SELECT
            c.contact_record_status
        FROM
            xrms.contacts c
        WHERE
            c.contact_id = %s;
    """

    bi_db_cursor = bi_db.cursor()
    bi_db_cursor.execute(IS_DELETED_SQL, [contact_id])
    is_deleted = bi_db_cursor.fetchone()
    bi_db_cursor.close()

    if is_deleted is not None:
        return is_deleted[0] != "a"
    else:
        return True


def get_contact_type_category_id(contact_id):
    """ Obtains the value of the contact_type_category_id field from the table contact_d """

    CONTACT_TYPE_CATEGORY_ID_QUERY = """
        SELECT
            contact_type_category_id
        FROM
            bi_warehouse_prd.contact_d
        WHERE
            contact_id = %s;
    """

    bi_db_cursor = bi_db.cursor()
    bi_db_cursor.execute(CONTACT_TYPE_CATEGORY_ID_QUERY, [contact_id])
    contact_type_category_id = bi_db_cursor.fetchone()[0]
    bi_db_cursor.close()

    return contact_type_category_id

def get_dupes_from_dump(db_file_name):
    """ Get a list of unique contactIDs from the provided database """
    
    GET_DUPES_QUERY = """
        SELECT DISTINCT ContactID, Email
        FROM mkto_dump
        WHERE (ContactID, FullName, Email) IN (
            SELECT ContactID, FullName, Email
            FROM mkto_dump
            GROUP BY ContactID, FullName, Email
            HAVING COUNT(*) > 1
        )
        AND ContactID IS NOT "null"
        AND Email IS NOT "null"
        ORDER BY CAST(ContactID AS UNSIGNED) ASC;
    """
    
    log.info(f"[MKTO_DEDUPE] step=get_dupes_from_dump action=start db_file={db_file_name}")
    sqlite_conn = sqlite3.connect(db_file_name)
    cursor = sqlite_conn.cursor()
    dupes = tuple(cursor.execute(GET_DUPES_QUERY).fetchall())
    log.info(f"[MKTO_DEDUPE] step=get_dupes_from_dump action=complete duplicates_count={len(dupes)}")
    sqlite_conn.close()
    return dupes

def soft_delete_contact(contact_id, loser, dry_run):
    """ Soft deletes a contact in email_marketing_f """
    
    if dry_run:
        return
    
    UPDATE_QUERY= """
        UPDATE 
            bi_warehouse_prd.email_marketing_f
        SET 
            row_status = 'Deleted', 
            deleted_at = NOW() 
        WHERE 
            contact_id = %s 
            AND marketo_id = %s
            AND row_status = 'Active' 
        LIMIT 1;
    """
        
    bi_db_cursor = bi_db.cursor()
    bi_db_cursor.execute(UPDATE_QUERY % (contact_id, loser))
    bi_db.commit()
    bi_db_cursor.close()


def show_EMF_record(contact_id, loser):
    """ Logs the important values of contact stored in email_marketing_f """

    SELECT_QUERY= """
        SELECT 
            email_marketing_f_id,
            contact_id,
            marketo_id,
            submitted_email,
            row_status,
            deleted_at
        FROM 
            bi_warehouse_prd.email_marketing_f
        WHERE 
            contact_id = %s
            AND marketo_id = %s;
    """

    bi_db_cursor = bi_db.cursor()
    bi_db_cursor.execute(SELECT_QUERY % (contact_id, loser))
    records = bi_db_cursor.fetchall()
    bi_db_cursor.close()
    log.info(records)
