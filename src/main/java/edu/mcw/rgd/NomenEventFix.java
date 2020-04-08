package edu.mcw.rgd;

import edu.mcw.rgd.dao.impl.GeneDAO;
import edu.mcw.rgd.dao.impl.NomenclatureDAO;
import edu.mcw.rgd.dao.impl.RGDManagementDAO;
import edu.mcw.rgd.dao.spring.GeneQuery;
import edu.mcw.rgd.dao.spring.IntListQuery;
import edu.mcw.rgd.dao.spring.NomenclatureEventsQuery;
import edu.mcw.rgd.datamodel.*;
import edu.mcw.rgd.process.Utils;

import java.util.*;
import java.util.Map;

public class NomenEventFix {

    NomenclatureDAO dao = new NomenclatureDAO();

    public static void main(String[] args) throws Exception {

        try {
            new NomenEventFix().run2();
//            new NomenEventFix().run();
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    void run2() throws Exception {

        RGDManagementDAO rdao = new RGDManagementDAO();
        int speciesTypeKey = 3;
        System.out.println("SPECIES: "+ SpeciesType.getCommonName(speciesTypeKey));
        String sql = "select * from genes g "+
        "where exists(select 1 from rgd_ids i where i.rgd_id=g.rgd_id and object_status='ACTIVE' and species_type_key=? and gene_type_lc not in('allele','splice')) "+
        "and not exists(select 1 from nomen_events e where e.rgd_id=g.rgd_id) and gene_source='NCBI'";
        List<Gene> genes = GeneQuery.execute(dao, sql, speciesTypeKey);
        System.out.println("NCBI missing events: "+genes.size());
        for( Gene g: genes ) {
            RgdId id = rdao.getRgdId2(g.getRgdId());
            String sql2 = "INSERT INTO nomen_events (nomen_event_key,rgd_id,symbol,name,ref_key,nomen_status_type,description,event_date,original_rgd_id) VALUES (?,?,?,?,?,?,?,?,?)";
            int refKey = 1724;
            dao.update(sql2, generateNomenEventKey(), g.getRgdId(), g.getSymbol(), g.getName(), refKey, "PROVISIONAL", "Symbol and Name status set to provisional", id.getCreatedDate(), g.getRgdId());
        }

        sql = "select * from genes g "+
                "where exists(select 1 from rgd_ids i where i.rgd_id=g.rgd_id and object_status='ACTIVE' and species_type_key=? and gene_type_lc not in('allele','splice')) "+
                "and not exists(select 1 from nomen_events e where e.rgd_id=g.rgd_id) and gene_source='Ensembl'";
        genes = GeneQuery.execute(dao, sql, speciesTypeKey);
        System.out.println("Ensembl missing events: "+genes.size());
        for( Gene g: genes ) {
            RgdId id = rdao.getRgdId2(g.getRgdId());
            String sql2 = "INSERT INTO nomen_events (nomen_event_key,rgd_id,symbol,name,ref_key,nomen_status_type,description,event_date,original_rgd_id) VALUES (?,?,?,?,?,?,?,?,?)";
            int refKey = 20683;
            dao.update(sql2, generateNomenEventKey(), g.getRgdId(), g.getSymbol(), g.getName(), refKey, "PROVISIONAL", "Symbol and Name status set to provisional", id.getCreatedDate(), g.getRgdId());
        }

    }

    void run() throws Exception {

        int round = 1;
        int rcount;
        do {
            rcount = propagateEvents();
            System.out.println("ROUND "+round+"   = "+rcount+"        "+new Date());
            round++;
        }
        while( rcount>0 );

        System.out.println("\n======\nALL ROUNDS COMPLETE\n");

        fixNomenEventKeys();
    }

    public int propagateEvents() throws Exception {

        // list of RGD_IDS from original table
        String sql1 = "SELECT DISTINCT rgd_id FROM tmp_nomen_events_20200226";
        List<Integer> rgdIdList = IntListQuery.execute(dao, sql1);
        Collections.shuffle(rgdIdList);
        int rcount = rgdIdList.size();
        int row = 0;
        for( int rgdId: rgdIdList ) {

            row++;
            String sql2 = "SELECT * FROM tmp_nomen_events_20200226 WHERE rgd_id=?";
            NomenclatureEventsQuery q = new NomenclatureEventsQuery(dao.getDataSource(), sql2);
            List<NomenclatureEvent> origEvents = dao.execute(q, rgdId);

            String sql3 = "SELECT * FROM nomen_events WHERE rgd_id=?";
            NomenclatureEventsQuery q3 = new NomenclatureEventsQuery(dao.getDataSource(), sql3);
            List<NomenclatureEvent> inRgdEvents = dao.execute(q3, rgdId);

            Map<Integer, NomenclatureEvent> inRgdEventsMap = new HashMap<>();
            for( NomenclatureEvent e: inRgdEvents ) {
                int k = hashCode(e);
                inRgdEventsMap.put(k, e);
            }

            for( NomenclatureEvent e: origEvents ) {
                int k = hashCode(e);
                NomenclatureEvent inRgdEv = inRgdEventsMap.get(k);
                if( inRgdEv!=null ) {
                    // full match: delete line from original table
                    String sql = "DELETE FROM tmp_nomen_events_20200226 WHERE nomen_event_key=?";
                    dao.update(sql, e.getNomenEventKey());
                    //System.out.println(row+"/"+rcount+". event matches for RGD:"+e.getRgdId());
                    break;
                } else {
                    // orig event not in RGD
                    //
                    // CASE 1: no events in RGD, only one orig event
                    if( inRgdEvents.size()==0 ) {
                        // add orig event, and delete the event from tmp table
                        dao.createNomenEvent(e);
                        String sql = "DELETE FROM tmp_nomen_events_20200226 WHERE nomen_event_key=?";
                        dao.update(sql, e.getNomenEventKey());
                        //System.out.println(row+"/"+rcount+". single event added for RGD:" + e.getRgdId());
                        break;
                    }
                    // CASE 2: no events in RGD, only one orig event
                    else {
                        // add orig event, and delete the event from tmp table
                        dao.createNomenEvent(e);
                        String sql = "DELETE FROM tmp_nomen_events_20200226 WHERE nomen_event_key=?";
                        dao.update(sql, e.getNomenEventKey());
                        //System.out.println(row+"/"+rcount+". multi event added for RGD:"+e.getRgdId());
                    }
                }
            }

            if( row>=2000 ) {
                break;
            }
        }
        return rcount;
    }

    public void fixNomenEventKeys() throws Exception {

        int keysRewritten = 0;

        // list of RGD_IDS from original table
        String sql1 = "SELECT DISTINCT rgd_id FROM nomen_events";
        List<Integer> rgdIdList = IntListQuery.execute(dao, sql1);
        Collections.shuffle(rgdIdList);
        int rcount = rgdIdList.size();
        int row = 0;
        for( int rgdId: rgdIdList ) {

            row++;
            String sql2 = "SELECT * FROM nomen_events WHERE rgd_id=? ORDER BY event_date,nomen_event_key";
            NomenclatureEventsQuery q = new NomenclatureEventsQuery(dao.getDataSource(), sql2);
            List<NomenclatureEvent> origEvents = dao.execute(q, rgdId);

            // check if all keys are in order
            boolean keysInOrder = true;
            for (int i = 1; i < origEvents.size(); i++) {
                NomenclatureEvent ev1 = origEvents.get(i - 1);
                NomenclatureEvent ev2 = origEvents.get(i);
                if (ev1.getNomenEventKey() > ev2.getNomenEventKey()) {
                    keysInOrder = false;
                    break;
                }
            }

            if( keysInOrder ) {
                System.out.println(row+"/"+rcount+". in order for RGD:"+rgdId);
            } else {
                System.out.println(row+"/"+rcount+". NOT in order for RGD:"+rgdId);

                List<Integer> newKeys = generateNomenEventKeys(origEvents.size());
                for( int i = 0; i < origEvents.size(); i++ ) {
                    NomenclatureEvent ev = origEvents.get(i);
                    String sql = "UPDATE nomen_events SET nomen_event_key=? WHERE nomen_event_key=?";
                    int oldKey = ev.getNomenEventKey();
                    int newKey = newKeys.get(i);
                    System.out.println("        old="+oldKey+"   new="+newKey);
                    dao.update(sql, newKey, oldKey);
                    keysRewritten++;
                }
                System.out.println("        keys rewritten "+newKeys.size());
            }
        }
        System.out.println("=== DONE === keys rewritten: "+keysRewritten);
    }

    public List<Integer> generateNomenEventKeys(int count) throws Exception {
        String sql = "select e.nomen_event_key+1 from nomen_events e "+
        "where not exists( select 1 from nomen_events e2 where e2.nomen_event_key=e.nomen_event_key+1 ) "+
        "order by e.nomen_event_key";

        List<Integer> keys = IntListQuery.execute(dao, sql);
        return keys.subList(0, count);
    }

    public Integer generateNomenEventKey() throws Exception {
        if( _keyCache.isEmpty() ) {
            _keyCache.addAll(generateNomenEventKeys(12));
        }
        int key = _keyCache.remove(0);
        return key;
    }
    List<Integer> _keyCache = new ArrayList<>();

    public int hashCode(NomenclatureEvent e) {
        return e.getOriginalRGDId()
            ^ e.getRefKey().hashCode()
            ^ e.getNomenStatusType().hashCode()
            ^ Utils.defaultString(e.getDesc()).hashCode()
            ^ Utils.defaultString(e.getNotes()).hashCode()
            ^ (e.getEventDate()==null?"":e.getEventDate().toString()).hashCode()
            ^ Utils.defaultString(e.getSymbol()).hashCode()
            ^ Utils.defaultString(e.getName()).hashCode()
            ^ Utils.defaultString(e.getPreviousSymbol()).hashCode()
            ^ Utils.defaultString(e.getPreviousName()).hashCode();
    }
}
