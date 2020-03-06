package edu.mcw.rgd;

import edu.mcw.rgd.process.Utils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.FileSystemResource;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author mtutaj
 * @since 12/22/11
 */
public class HistoricLogs {


    private final Logger log = Logger.getLogger("status");
    private String version;

    public static void main(String[] args) throws Exception {


        DefaultListableBeanFactory bf = new DefaultListableBeanFactory();
        new XmlBeanDefinitionReader(bf).loadBeanDefinitions(new FileSystemResource("properties/AppConfigure.xml"));
        HistoricLogs manager = (HistoricLogs) (bf.getBean("manager"));
        try {
            manager.run();
        } catch(Exception e) {
            Utils.printStackTrace(e, manager.log);
            throw e;
        }
    }

    void run() throws Exception {

        Date now = new Date();

        log.info(this.getVersion());
        //log.info("   "+dao.getConnectionInfo());

        SimpleDateFormat sdt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        log.info("   started at "+sdt.format(now));


        String rootDirName = "/home/rgddata/pipelines";
        File rootDir = new File(rootDirName);
        File[] pipelineDirs = rootDir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                File aDir = new File(dir.getAbsolutePath()+"/"+name);
                if( aDir.isDirectory() ) {
                    return true;
                }
                return false;
            }
        });

        // look for 'data/archive' and 'logs/archive' subdirectories
        for( File pipelineDir: pipelineDirs ) {
            String pipelineName = pipelineDir.getName();

            // ensure the pipeline directory is out there
            String outDirName = "data/"+pipelineName;

            File inDataDir = new File(pipelineDir.getAbsolutePath()+"/data/archive");
            copyFilesInDir(outDirName+"/data", inDataDir);

            File inLogsDir = new File(pipelineDir.getAbsolutePath()+"/logs/archive");
            copyFilesInDir(outDirName+"/logs", inLogsDir);
        }

        log.info("--SUCCESS-- elapsed "+ Utils.formatElapsedTime(now.getTime(), System.currentTimeMillis()));
    }

    void copyFilesInDir(String outDirName, File inDir) throws IOException {
        if( inDir.exists() ) {
            //ensure out dir exists
            File outDir = new File(outDirName);
            if( !outDir.exists() ) {
                outDir.mkdirs();
            }

            File[] arFiles = inDir.listFiles();
            for( File arFile: arFiles ) {
                if( arFile.isFile() ) {
                    Path srcPath = Paths.get(arFile.getAbsolutePath());
                    Path dstPath = Paths.get(outDirName+"/"+arFile.getName());
                    System.out.println(srcPath.toFile().getAbsolutePath()+"  --> "+dstPath.toFile().getAbsolutePath());
                    Files.copy(srcPath, dstPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.COPY_ATTRIBUTES);
                }
            }
        }
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getVersion() {
        return version;
    }
}
