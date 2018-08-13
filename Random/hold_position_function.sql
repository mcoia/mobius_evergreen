DROP FUNCTION action.hold_request_queue_pos(bigint);                           
                                                                               
CREATE OR REPLACE FUNCTION action.hold_request_queue_pos(hold_id BIGINT)       
RETURNS BIGINT AS $$                                                           
    SELECT subq.queue_position FROM (                                          
        WITH related_holds AS (                                                
            SELECT                                                             
                ahr.id AS hold_id,                                             
                COALESCE(ahr.cut_in_line, FALSE) AS cut_in_line,               
                ahr.request_time                                               
            FROM reporter.hold_request_record rhrr                             
            JOIN reporter.hold_request_record rhrr_related                     
                ON (rhrr_related.bib_record = rhrr.bib_record)                 
            JOIN action.hold_request ahr ON (ahr.id = rhrr_related.id)         
            WHERE rhrr.id = $1                                                 
                AND ahr.cancel_time IS NULL                                    
                AND ahr.fulfillment_time IS NULL                               
        ) SELECT ROW_NUMBER() OVER (                                           
            ORDER BY cut_in_line DESC, request_time                            
        ) AS queue_position, hold_id FROM related_holds                        
    ) subq WHERE subq.hold_id = $1                                             
$$ LANGUAGE SQL; 