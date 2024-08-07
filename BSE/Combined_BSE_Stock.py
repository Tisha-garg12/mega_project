import requests
import pandas as pd
import datetime
import io
import time

def fetch_stock_data(symbol, start_date, end_date):
    url = f"https://query1.finance.yahoo.com/v7/finance/download/{symbol}?period1={start_date}&period2={end_date}&interval=1d&events=history&includeAdjustedClose=true"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200: 
        data = response.content.decode('utf-8')
        df = pd.read_csv(io.StringIO(data))
        return df
    else:
        raise Exception(f"Failed to fetch data for {symbol}. Status code: {response.status_code}")

def get_epoch_time(date_str):
    date = datetime.datetime.strptime(date_str, '%Y-%m-%d')
    return int(date.timestamp())

start_date = '2015-01-01'
end_date = '2024-06-30'
start_epoch = get_epoch_time(start_date)
end_epoch = get_epoch_time(end_date)


bse_stock_symbols = ['CALSOFT.BO', 'SABOOSOD.BO','MANALIPETC.BO','ASTERDM.BO','ATULAUTO.BO','NAUKRI.BO','SOUTLAT.BO',
             'TNSTLTU.BO','BKV.BO','GENUSPAPER.BO','RAPICUT.BO','MODRNSH.BO','CHRTEDCA.BO','TEEAI.BO','GULPOLY.BO','SAH.BO','SICAGEN.BO'
             ,'GREENCREST.BO','GOPAIST.BO','SHARDACROP.BO','SINGER.BO','SMFIL.BO','ALPHAGEO.BO','SEPC.BO','INOXWIND.BO',
            'TAINWALCHM.BO','ARENTERP.BO','MYSORPETRO.BO','MDRNSTL.BO','PARSHWANA.BO','VIRAT.BO','CHDCHEM.BO',
             'SRESTHA.BO','NAM.BO','VALSONQ.BO','IWEL.BO','SAMRATPH.BO','ADL.BO','JYOTI.BO','KIRANVYPAR.BO',
            'ANJANIFIN.BO','ASHSI.BO','SURYVANSP.BO','OAL.BO','IPRINGLTD.BO',
             'KAJARIR.BO','MORARKFI.BO','ADIEXRE.BO','MANORG.BO','ZENITHHE.BO','SUPERSPIN.BO','HRYNSHP.BO','LYKALABS.BO',
             'SIMRAN.BO','PURPLE.BO','DREDGECORP.BO','ARCHITORG.BO','MEGRISOFT.BO','BRNL.BO','HARDWYN.BO','PEL.BO','PROZONER.BO','EVERESTIND.BO',
             'AMRAAGRI.BO','HEXATRADEX.BO','SEASONST.BO','RIIL.BO','JAICORPLTD.BO','ORIENTLTD.BO','INDIANVSH.BO','CINDRELL.BO',
             'GAGAN.BO','PROFINC.BO','SARTHAKIND.BO','ASMS.BO','INTEGFD.BO','INDOWIND.BO','GREAVESCOT.BO','KGPETRO.BO','TEJASNET.BO'
             ,'FINELINE.BO','ACEMEN.BO','PARAGONF.BO','INDIANCARD.BO','VIVIDIND.BO','THIRDFIN.BO','NATRAJPR.BO',
              'MUKESHB.BO','INOXGREEN.BO','GSS.BO','YATRA.BO','AMFORG.BO','INDTERRAIN.BO',
            'MINALIND.BO','DIGIDRIVE.BO','GUJCOTEX.BO','SPMLINFRA.BO','KCLINFRA.BO','INDCEMCAP.BO',
            'PACIFICI.BO','PRSMJOHNSN.BO','DCMNVL.BO','UNISHIRE.BO','AMDIND.BO','BMAL.BO','RASRESOR.BO',
              'IMAGICAA.BO','POLYMAC.BO','SADHNA.BO','LADDERUP.BO','BHEL.BO','PCS.BO','BARODARY.BO','RADIOCITY.BO',
             'BLUECHIPT.BO','HBPOR.BO','KENFIN.BO','ADCON.BO','PNC.BO','TOKYOPLAST.BO','UPL.BO','SWAGTAM.BO',
              'HINDMILL.BO','OLPCL.BO','GLOSTERLTD.BO','DIANATEA.BO','YUVRAAJHPL.BO','ZENIFIB.BO','APMIN.BO',
         'RAJESHEXPO.BO','VISAKAIND.BO','INTELSOFT.BO','ANKIN.BO','ALLCARGO.BO',
             'JPASSOCIAT.BO','RAMCOIND.BO','DHAMPURE.BO','BODALCHEM.BO','FCSSOFT.BO','MAHEPC.BO','UNITEDTE.BO','HBLEAS.BO','GUJAPOLLO.BO',
              'NAHARINDUS.BO','MARSONS.BO','SPS.BO','JINDALPOLY.BO','UMESLTD.BO','HINFLUR.BO','CONFINT.BO','ENERGYDEV.BO','MAHLOG.BO',
             'HGS.BO','PHOTON.BO','ROSEMER.BO','WINDMACHIN.BO','TRANSFRE.BO','IWP.BO','RITESHIN.BO','3PLAND.BO',
            'KOTHARIPRO.BO','SPCAPIT.BO','KRISHNACAP.BO','COFFEEDAY.BO','SAMBHAAV.BO','GSTL.BO','SUNLOC.BO','SWSOLAR.BO','SHIVAMAUTO.BO',
            'ANIKINDS.BO','ZDHJERK.BO','NDLVENTURE.BO','CINDHO.BO','FRONTCORP.BO','GUJPETR.BO','GKCONS.BO','GITARENEW.BO','PQIF.BO','BERYLSE.BO',
            'ACROW.BO','SUNGOLD.BO','ATFL.BO','DIVSHKT.BO','YASHCHEM.BO','GUJINJEC.BO','RHL.BO','SHVFL.BO','ASIANTNE.BO',
             'ATVPR.BO','SANCTRN.BO','SURANASOL.BO','KINETRU.BO','THACKER.BO','SOMATEX.BO','DSINVEST.BO','UCAL.BO',
             'MODINATUR.BO','VHLTD.BO','KARMAENG.BO','PHOENXINTL.BO','NEOINFRA.BO','MAITRI.BO','LONTE.BO','EMPOWER.BO','NEWINFRA.BO','HAVISHA.BO',
            'MARBU.BO','ISHANCH.BO','DHPIND.BO','GCMCAPI.BO','TATIAGLOB.BO','SHIVA.BO','MOTOGENFIN.BO','HATHWAY.BO','OMNITEX.BO',
             'RAJTV.BO','ENTRINT.BO','SCANPGEOM.BO','JSHL.BO','VICTMILL.BO','SUPREME.BO','SADBHAV.BO','DEEPAKSP.BO','ACME.BO',
              'GRAVISSHO.BO','UNITEDINT.BO','PANKAJPIYUS.BO','SILINV.BO','GEMSI.BO','SAICOM.BO','GINNIFILA.BO',
            'GGL.BO','MAHLIFE.BO','PILANIINVS.BO','BHARATAGRI.BO','KRETTOSYS.BO','SIMBHALS.BO','BENGALT.BO', 'SIMPLXREA.BO', 'YOGISUNG.BO', 'ORIENTPPR.BO', 'SARVOTTAM.BO',
            'NPRFIN.BO', 'SIDDHA.BO', 'RAIN.BO', 'HIL.BO', 'MIDEASTP.BO', 'ASIAPAK.BO' ,'LOVABLE.BO' , 'BRIGHTBR.BO',  'TARC.BO', 'EXCELINDUS.BO', 'URJA.BO', 'KJMCFIN.BO', 'INDOEURO.BO' ,'TRIVENIENT.BO','POLYPLEX.BO' ,
                ,'ZENITHEXPO.BO', 'RAGHUNAT.BO', 'ZOMATO.BO',  'BASML.BO', 'KANANIIND.BO', 'POLICYBZR.BO', 'LAFFANSQ.BO',  'LIBORDFIN.BO', 'SHBAJRG.BO', 'SCILAL.BO', 'MUNOTHFI.BO' ,'SWOEF.BO' ,'NCLRESE.BO' ,'TATAINVEST.BO', 'TITANSEC.BO' ,'TREJHARA.BO',
            'BHARATGEAR.BO' ,'VIJIFIN.BO' , 'WINSOMBR.BO', 'JAMESWARREN.BO', 'SIGNATURE.BO' ,'ANG.BO' ,'CYBELEIND.BO' ,'GLFL.BO' ,'KOLTEPATIL.BO' , 'JIOFIN.BO', 'UNIENTER.BO' ,'STEL.BO' ,
        'SPICEJET.BO' ,'PPAP.BO' ,'HOCL.BO', 'VIJSOLX.BO', 'TARMAT.BO' ,'STARLOG.BO', 'GRAPHITE.BO',  'RPOWER.BO' ,'MERCANTILE.BO' ,'SKRABUL.BO' , 'SUMMITSEC.BO',
          'BAZELINTER.BO','KHANDSE.BO' ,'VANDANA.BO', 'AVANCE.BO', 'ATHARVENT.BO' ,'DYNAMICP.BO' ,'AGSTRA.BO' ,'SAGCEM.BO' ,'CAMLINFINE.BO' ,'SHYAMAINFO.BO' ,'HARYNACAP.BO',  'BAMPSL.BO' ,'BHARAT.BO' ,'PMTELELIN.BO' ,'NAGPI.BO' ,
           'VAGHANI.BO', 'USHAKIRA.BO' ,'ERPSOFT.BO' ,'NAHARCAP.BO', 'COSYN.BO' ,'ASIANTILES.BO' ,'NAVKARCORP.BO', 'GGPL.BO', 'BI.BO' ,'TELECANOR.BO' ,'OIVL.BO' ,'MANGALAM.BO', 'MAGNUM.BO', 'KHOOBSURAT.BO' ,'ALCHCORP.BO' ,'GANGESSECU.BO' ,'PANJON.BO'  ,'LEENEE.BO' ,
           'WELINV.BO', 'JYOTISTRUC.BO', 'BHILSPIN.BO', 'INDSILHYD.BO', 'NATHUEC.BO', 'KAKATCEM.BO', 'MYSTICELE.BO',  'ACIIN.BO', 'KICL.BO', 'FILATFASH.BO', 'BFFL.BO', 'VALIANTORG.BO', 'ASAHISONG.BO', 'PALREDTEC.BO', 
            'ALSTONE.BO', 'MPAGI.BO', 'OTCO.BO', 'PEARLPOLY.BO', 'SHYAMCENT.BO', 'VEERENRGY.BO', 'PANKAJPO.BO', 'BOMDYEING.BO', 'ORIENTBELL.BO', 'RAJPALAYAM.BO', 'TARINI.BO', 'TIPSFILMS.BO', 'INDINFO.BO',  'MANGIND.BO', 'SEQUENT.BO', 'MAHSCOOTER.BO', 'REGENTRP.BO' ,
              'SHAQUAK.BO', 'MAXESTATES.BO', 'SRDAPRT.BO', 'KANORICHEM.BO', 'MAHANIN.BO', 'CCFCL.BO', 'MAXHEIGHTS.BO',  'SAMBANDAM.BO', 'SYMBIOX.BO', 'BAJAJHIND.BO', 'SHIVACEM.BO',  'SUMERUIND.BO', 'RAMANEWS.BO', 'TELESYS.BO',
            'TVSELECT.BO', 'ADITYA.BO', 'KIRANPR.BO', 'KANCHI.BO', 'SANBLUE.BO', 'JSWHL.BO',  'ARCFIN.BO', 'BITS.BO', 'EXCEL.BO',  'HARIAEXPO.BO',   'MINOLTAF.BO', 'ARSSINFRA.BO', 'SMIFS.BO' ,
               'AVI.BO', 'ALFAVIO.BO',  'CISTRO.BO', 'VMART.BO', 'MAHACORP.BO', 'RLF.BO', 'KBSINDIA.BO', 'YAMNINV.BO',  'ALFREDHE.BO', 'MOHITIND.BO', 'OSWALAGRO.BO', 'VIKALPS.BO',  'TEXINFRA.BO', 'OSWALGREEN.BO',
                'NAHARSPING.BO', 'DAICHI.BO',  'SIPTL.BO', 'MCCHRLS-B.BO', 'ABFRL.BO', 'SETUINFRA.BO', 'MARGOFIN.BO', 'DEEPENR.BO', 'UNICHEMLAB.BO',  'DEVINE.BO', 'VCU.BO', 'DWL.BO', 'HMVL.BO', 'RLFL.BO', 'TIRUMALCHM.BO', 
                  'VITESSE.BO', 'RKDL.BO', 'KKFIN.BO', 'WALCHANNAG.BO', 'PANAFIC.BO', 'MUKTA.BO', 'LAKPRE.BO', 'TRIMURTHI.BO', 'EXPLICITFIN.BO','SHUKJEW.BO', 'ZMILGFIN.BO', 'ASHRAM.BO', 
                     'TTIL.BO',  'SKIL.BO', 'JAIHINDS.BO', 'STHINPA.BO', 'SUNDARAM.BO',   'VINTAGES.BO',   'DFL.BO', 'SUPERIOR.BO', 
                      'NIRAVCOM.BO', 'CLIOINFO.BO', 'GOLDLINE.BO',  'VBIND.BO', 'TV18BRDCST.BO', 'SAMPANN.BO',  'ELEFLOR.BO', 'SHALPRO.BO', 'ZSVARAJT.BO',  'VISIONCO.BO',  'TREEHOUSE.BO', 'VKJINFRA.BO', 'INDRENEW.BO' ,
                          'GUJCRED.BO', 'DCAL.BO', 'SIDDHEGA.BO', 'COMPUPN.BO', 'AVAILFC.BO', 'BLSINFOTE.BO', 'GLOBALCA.BO', 'ASITCFIN.BO', 'PIFL.BO', 'JAINCO.BO', 'MFLINDIA.BO',  'VAXHS.BO', 'WAGEND.BO','SHARPINV.BO', 'KARNAVATI.BO',
                           'TIRTPLS.BO', 'GEETANJ.BO', 'GTNTEX.BO',  'CCHHL.BO', 'HTMEDIA.BO', 'MADHAV.BO', 'HUBTOWN.BO', 'NACLIND.BO', 'NAHARPOLY.BO', 'EDSL.BO', 'PREMCAP.BO', 'DICIND.BO', 
                            'SHEMAROO.BO', 'KEL.BO', 'HANSUGAR.BO', 'PRISMMEDI.BO', 'LOOKS.BO', 'SILVOAK.BO', 'LAHL.BO', 'DOLPHIN.BO', 'SUNCITYSY.BO', 'THAKDEV.BO', 'INTERDIGI.BO',  'KEMP.BO', 'DIAMANT.BO', 'NETWORK18.BO', 'VSFPROJ.BO',
                    'KAPILRAJ.BO', 'VLEGOV.BO', 'GFLLIMITED.BO', 'RSWM.BO', 'SEYAIND.BO',  'SIMPLEXINF.BO','DELTA.BO', 'POLYSPIN.BO', 'TERAI.BO', 'KFBL.BO', 'VERANDA.BO', 'LOTUSCHO.BO', 'TECHIN.BO', 'FORINTL.BO', 'SALEM.BO',
                     'SHIVATEX.BO', 'SYTIXSE.BO', 'NORTHLINK.BO', 'SVCIND.BO', 'ACLGATI.BO', 'KLGCAP.BO', 'OMEGAIN.BO', 'PFOCUS.BO', 'AJEL.BO', 'GOENKA.BO', 'DARJEELING.BO', 'SVAMSOF.BO', 'RAJGASES.BO', 'TAVERNIER.BO', 'USGTECH.BO', 
                      'MBLINFRA.BO', 'POLOHOT.BO', 'PANINDIAC.BO', 'EASTRED.BO', 'GARWSYN.BO',    'LOYALTEX.BO', 'SFPIL.BO', 'ORICONENT.BO', 'NNTL.BO', 'SSLEL.BO', 'ICSL.BO', 'ASTRON.BO', 'INDIACEM.BO',
                       'GARWAMAR.BO',  'JETKINGQ.BO', 'RVHL.BO', 'GOLDTECH.BO', 'SIPL.BO', 'SHREYAS.BO', 'PATIDAR.BO', 'SHANTAI.BO', 'TULSYAN.BO', 'DOLPHMED.BO', 'WOCKPHARMA.BO', 'OMNIAX.BO', 'TRIOMERC.BO', 'VISESHINFO.BO',
                        'JAMSHRI.BO', 'SUJALA.BO', 'AGIOPAPER.BO',  'ASMTEC.BO', 'NOUVEAU.BO', 'AMTL.BO', 'VOLLF.BO', 'RICHUNV.BO',  'SWORDEDGE.BO', 'VAARAD.BO', 'JSTL.BO', 'MEDICAPQ.BO', 'GANGOTRI.BO', 'VISAGAR.BO',  'SCC.BO', 'SSWRL.BO', 'ACEEDU.BO' ,
                         'GARNETINT.BO', 'SATRAPROP.BO', 'GNRL.BO', 'STCORP.BO', 'GKB.BO', 'TCLCONS.BO', 'KUSHIND.BO', 'PRIMAIN.BO', 'MPILCORPL.BO', 'LAKSHMIMIL.BO', 'VISIONCINE.BO', 'INOVSYNTH.BO',  'IISL.BO', 'DELHIVERY.BO', 'PADMALAYAT.BO', 
                          'HEMIPROP.BO', 'WARRENTEA.BO', 'GATECHDVR.BO', 'KATRSPG.BO',  'SAMTEX.BO', 'ORISSAMINE.BO', 'SAYAJIIND.BO', 'GOLCA.BO','STELLAR.BO', 'LCCINFOTEC.BO', 
                           'PARASPETRO.BO', 'JHS.BO', 'AFEL.BO', 'WESTLEIRES.BO',  'MADHUCON.BO', 'VALIANTLAB.BO',  'SALORAINTL.BO', 'VIJAYTX.BO',   'PCJEWELLER.BO', 'SAGL.BO', 'HUIL.BO', 'NGIL.BO', 'KIRIINDUS.BO',
                            'EQUIPPP.BO', 'BGLOBAL.BO', 'HITKITGLO.BO', 'VELHO.BO', 'LANDMARC.BO', 'SSPDL.BO', 'SUNCLAY.BO', 'SAICAPI.BO', 'CHEMPLASTS.BO', 'SANWARIA.BO', 'KAVVERITEL.BO', 'TRABI.BO', 'KIOCL.BO',
                   'SICALLOG.BO', 'TAAZAINT.BO', 'PVP.BO', 'GOKAKTEX.BO', 'HATHWAYB.BO', 'GVFILM.BO', 'JLL.BO', 'SHAMROIN.BO', 'CATVISION.BO', 'UTLINDS.BO', 'ARCHIES.BO', 'TGBHOTELS.BO', 'SVGLOBAL.BO',  'IFBAGRO.BO', 'APCL.BO' ,
          'ARSHIYA.BO', 'PATSPINLTD.BO', 'PALASHSECU.BO', 'BLUECHIP.BO', 'GCMSECU.BO', 'SUNRAJDI.BO', 'HASTIFIN.BO', 'KBCGLOBAL.BO',  'LORDSCHLO.BO', 'BPCAP.BO',  'LUDLOWJUT.BO' ,
          'PREMSYN.BO', 'UNIVARTS.BO', 'DML.BO',  'VIRTUALG.BO', 'VENLONENT.BO', 'NILACHAL.BO', 'GTNINDS.BO',  'VIKASWSP.BO', 'MOL.BO', 'VIPCLOTHNG.BO', 'MMRUBBR-B.BO' ,
             'BKMINDST.BO', 'DHARFIN.BO', 'FORTISMLR.BO', 'KAMANWALA.BO', 'CNOVAPETRO.BO',  'GOLKONDA.BO', 'NOGMIND.BO', 'ASYL.BO', 'ELANGO.BO', 'PRAGBOS.BO', 'SHIVAMILLS.BO', 'SUPRATRE.BO', 'RCCL.BO', 'ARAVALIS.BO', 'CEETAIN.BO', 'ROYALIND.BO', 'IDEA.BO',
            'PANACEABIO.BO',  'EROSMEDIA.BO', 'IPOWER.BO', 'YASHMGM.BO', 'SKYLMILAR.BO', 'GUJINV.BO', 'WILLIMFI.BO', 'INDOKEM.BO', 'ISFL.BO', 'GUJALKALI.BO', 'AKSHARCHEM.BO', 'STANROS.BO',   'FRUTION.BO', 
            'AANCHALISP.BO', 'LGBFORGE.BO', 'BORORENEW.BO', 'PODDARHOUS.BO', 'CAPRICORN.BO', 'KRISHNA.BO', 'NURECA.BO',  'VANTABIO.BO', 'SHRISTI.BO', 'DCMFINSERV.BO', 'EMAMIREAL.BO',  'NDTV.BO', 'KALLAM.BO', 'ESTER.BO', 'MARIS.BO' ,
           'SGL.BO',  'DHENUBUILD.BO', 'SVPGLOB.BO', 'PHOTOQUP.BO', 'ALOKINDS.BO', 'ANSINDUS.BO', 'LINKPH.BO', 'ASTEC.BO',  'RADHEDE.BO', 'INDORAMA.BO', 'VJLAXMIE.BO', 'KASHYAP.BO', 'PHYTO.BO', 'PHRMASI.BO', 'RBA.BO', 'FRSHTRP.BO', 'WATERBASE.BO' ,
           'ABHIINFRA.BO', 'BACPHAR.BO', 'IEL.BO',  'LIPPISYS.BO', 'MINFY.BO', 'PRAENG.BO', 'PARSVNATH.BO', 'MILLENNIUM.BO', 'KGDENIM.BO', 'FLEXFO.BO', 'DHANI.BO', 'HDIL.BO', 'SANGHIIND.BO', 'SUBEXLTD.BO', 'RELIABVEN.BO', 'HINDALUMI.BO' ,
           'BANG.BO', 'GLOBOFFS.BO', 'SHAHALLOYS.BO', 'STRGRENWO.BO', 'NOL.BO', 'OSWAYRN.BO', 'POLYTEX.BO', 'TEXELIN.BO', 'ISWL.BO', 'KLIFESTYL.BO', 'TIJARIA.BO', 'HIPOLIN.BO', 'CONTAINER.BO', 'SHREYASI.BO', 'IBRIGST.BO', 'MNIL.BO', 'RAMAPHO.BO',
          'LIMECHM.BO',  'BAROEXT.BO', 'QUANTBUILD.BO', 'CREATIVEYE.BO', 'SANTOSHF.BO', 'COMPINFO.BO', 'ACL.BO', 'CHOKSI.BO', 'SUTLEJTEX.BO', 'FUTURSEC.BO', 'SYBLY.BO', 'JAUSPOL.BO', 'ROLTA.BO', 'LASA.BO', 'SML.BO', 'SCBL.BO', 'PBMPOLY.BO', 'SIBARAUT.BO',
         'MEHTAHG.BO', 'SIL.BO', 'NIYOGIN.BO', 'VALLABHSQ.BO', 'MCLEODRUSS.BO', 'UNRYLMA.BO', 'SHYMINV.BO', 'VISASTEEL.BO', 'TCMLMTD.BO', 'SRAMSET.BO', 'ORIENTALTL.BO', 'WILLAMAGOR.BO', 'JCHAC.BO', 'STDBAT.BO',
          'KHAICHEM.BO', 'INDAGIV.BO', 'OONE.BO', 'SECMARK.BO', 'NIKKIGL.BO', 'TCIIND.BO', 'JAYSREETEA.BO', 'KANELIND.BO', 'STEP2COR.BO', 'DELTAMAGNT.BO', 'CMICABLES.BO', 'GILLANDERS.BO', 'MTNL.BO', 'RASANDIK.BO', 'MTEDUCARE.BO', 'ITI.BO', 'RAJPACK.BO', 'INDIANACRY.BO'
          , 'PADMAIND.BO', 'CHMBBRW.BO', 'SOLARA.BO', 'NSLNISP.BO', 'ZODIACLOTH.BO', 'VISTAPH.BO', 'TWINSTAR.BO', 'CONTICON.BO', 'SPSINT.BO', 'ADVIKLA.BO', 'STARLIT.BO', 'BIBCL.BO'
           , 'FRASER.BO', 'PUNJCOMMU.BO', 'RAP.BO', 'INTECCAP.BO', 'KOCL.BO', 'NAGTECH.BO', 'GOLDCOINHF.BO', 'INNOCORP.BO', 'AARVEEDEN.BO', 'JCTLTD.BO', 'UNIOFFICE.BO', 'PAYTM.BO', 'RESTILE.BO', 'SPENCERS.BO', 'NEXTMEDIA.BO',
              'FCONSUMER.BO', 'APTPACK.BO', 'OMKARCHEM.BO', 'AHMDSTE.BO', 'DECNGOLD.BO', 'ECOBOAR.BO', 'STURDY.BO', 'EMBDL.BO', 'GALAGEX.BO', 'NOIDATOLL.BO', 'MINAXI.BO', 'KEERTHI.BO', 'DHANFAB.BO', 'HEMANG.BO', 'DAIKAFFI.BO', 'TSPIRITUAL.BO', 'BHEEMACEM.BO',
                'FACORALL.BO', 'SRIND.BO', 'AURUM.BO', 'RAMGOPOLY.BO', 'SHALPAINTS.BO', 'ANDREWYU.BO', 'WELCURE.BO', 'TRITON.BO', 'ENVAIREL.BO', 'QUEST.BO', 'CEREBRAINT.BO', 'KISAN.BO', 'ORTEL.BO',
                     'ABCGAS.BO', 'FRETAIL.BO', 'NITCO.BO', 'JAYCH.BO', 'SAENTER.BO', 'SELMC.BO' ,'GOODRICKE.BO', 'RAMASIGNS.BO' ,'BGRENERGY.BO', 'GLITTEKG.BO', 'RELICTEC.BO', 'ASPIRA.BO', 'VAMA.BO', 'RAJVIR.BO', 'RAUNAQEPC.BO' ,
            'MSRINDIA.BO' , 'ARCEEIN.BO' ,'WINSOME.BO' ,'NIBL.BO', 'GAYAPROJ.BO' ,'WEBELSOLAR.BO' ,'ESHAMEDIA.BO' ,'RAVALSUGAR.BO' ,'MANUGRAPH.BO', 'UMIYA.BO', 'SECURKLOUD.BO' ,'RHFL.BO' ,'BCLENTERPR.BO', 'JPTSEC.BO' ,'SHESHAINDS.BO' ,
            'UNITECH.BO', 'BLOOM.BO', 'XELPMOC.BO', 'ZEEMEDIA.BO', 'GEPIL.BO', 'SUMEETINDS.BO', 'PRSNTIN.BO', 'MIRCELECTR.BO', 'KREBSBIO.BO', 'DTIL.BO', 'UNIVPHOTO.BO', 'OROSMITHS.BO', 'A2ZINFRA.BO',
          'TAKE.BO' ,'MEP.BO', 'PICCASUG.BO' ,'KERNEX.BO' ,'MORARJEE.BO' ,'OMAXE.BO' ,'KTIL.BO' ,'SABEVENTS.BO' ,'OILCOUNTUB.BO' ,'LPDC.BO'   , 'PADAMCO.BO' ,'SUVIDHAA.BO', 'GLOBUSCON.BO'
          , 'PANELEC.BO' ,'HEADSUP.BO' ,'BALKRISHNA.BO' ,'BRAWN.BO' ,'SEMAC.BO', 'SAFFRON.BO' ,'PHARMAID.BO', 'NIHARINF.BO', 'FLEXITUFF.BO' ,'HEMACEM.BO' ,'STERPOW.BO' ,'EUROTEXIND.BO' ,'TCNSBRANDS.BO' 
            , 'TARAPUR.BO', 'MARUTISE.BO' ,'AKSHOPTFBR.BO', 'ANKITMETAL.BO' ,'FERVENTSYN.BO' ,'SIKOZY.BO' ,'SUVEN.BO' ,'ADORMUL.BO' ,'PRAXIS.BO' ,'INDRAIND.BO' ,'AICHAMP.BO'  ,'AADIIND.BO', 'SPECTRA.BO',
             'QUINTEGRA.BO', 'HEMORGANIC.BO' ,'PARMAX.BO' ,'TRINITYLEA.BO' ,'TIL.BO' ,'SITINET.BO' ,'KUMPFIN.BO', 'RCIIND.BO' ,'SIMPLXPAP.BO', 'GOGIACAP.BO' ,'RAMCOSYS.BO', 'BLAL.BO', 'KAKTEX.BO'
             , 'BURNPUR.BO', 'KABRADG.BO' ,'TPROJECT.BO', 'TDSL.BO' , 'ONTIC.BO' ,'VIVIDHA.BO' , 'SOMAPPR.BO' ,'GRAVITY.BO', 'KAYA.BO', 'SILVERO.BO' ,'TRANSFD.BO' ,'SGNTE.BO' ,
             'RSCINT.BO', 'INCON.BO', 'FLFL.BO' , 'TARAI.BO' ,'JAIPAN.BO', 'LYPSAGEMS.BO', 'IYKOTHITE.BO', 'ORTINLAB.BO' ,'IMPEXFERRO.BO' ,'IDM.BO' ,'REGENCERAM.BO' ,'MELSTAR.BO', 'UNITINT.BO', 'PFLINFOTC.BO' ,'ROSELABS.BO',
            'ESSARSHPNG.BO', 'HEERAISP.BO' ,'SRMENERGY.BO' ,'NUTRICIRCLE.BO' ,'UNQTYMI.BO' , 'SPARC.BO', 'NAGAFERT.BO' ,'BEEYU.BO' ,'RISAINTL.BO' ,'STARCOM.BO' ,'SHAHFOOD.BO' ,'IITLPROJ.BO', 'SURFI.BO' 
          ,'STARLITE.BO', 'MPCOSEMB.BO' ,'PAEL.BO' ,'SHARP.BO' ,'ASINPET.BO' ,'STRLGUA.BO' ,'ANERI.BO'  ,'YAARI.BO' ,'SIELFNS.BO' ,'UNIVAFOODS.BO' ,'EMAINDIA.BO', 'KORE.BO' 
            , 'TRICOMFRU.BO' ,'MEDIAONE.BO'  ,'INDBNK.BO' ,'AMALGAM.BO'  ,'JAYBHCR.BO', 'TCIFINANCE.BO', 'QUADRANT.BO' ,'GUJSTATFIN.BO', 'VASINFRA.BO' ,'ELFORGE.BO', 'CARNATIN.BO' ,'GTLINFRA.BO' ,
           'SWITCHTE.BO' ,'RAYLA.BO' ,'ROYALCU.BO' ,'MODIPON.BO' ,'JETAIRWAYS.BO', 'CORNE.BO'  ,'NBFOOT.BO', 'SIMPLXMIL.BO' ,'ELAND.BO' ,'PRIYALT.BO' ,'MAL.BO' ,'UNIWORTH.BO' ,'MODWOOL.BO'  ,'SLSTLQ.BO'  ,'VARDMNPOLY.BO' ,'DECPO.BO',
            'TATAYODOGA.BO' ,'PRAKASHSTL.BO' ,'MUKATPIP.BO' ,'SPSL.BO', 'GTL.BO' ,'SHYAMTEL.BO', 'TNTELE.BO', 'SUDAI.BO', 'YASHRAJC.BO', 'FELDVR.BO' ,'TATAMTRDVR.BO' ,'BJDUP.BO', 'MIVENMACH.BO' ,'NATPLY.BO', 'GFSTEELS.BO' ,'TECILCHEM.BO'
         ,'TTIENT.BO', 'ISTRNETWK.BO', 'RAJSPTR.BO',  'RAJINFRA.BO', 'MPL.BO', 'INFOMEDIA.BO', 'EDUCOMP.BO', 'STCINDIA.BO', 'SABTNL.BO', 'SEATV.BO', 'RCOM.BO', 'KRIDHANINF.BO',
            'RAMAPETRO.BO', 'NIMBSPROJ.BO', 'MULLER.BO' ,'JISLDVREQS.BO', 'ABAN.BO' ,'ALPSINDUS.BO', 'ANSALAPI.BO', 'BINANIIND.BO' ,'CRANESSOFT.BO' ,'DIACABS.BO' ,'DISHTV.BO' ,'GOLDENTOBC.BO', 'HAZOOR.BO' ,'HMT.BO', 'MBECL.BO' 
           ,'PERMAGN.BO', 'TARSONS.BO', 'DDIL.BO', 'UNISTRMU.BO', 'GAYAHWS.BO', 'TVVISION.BO', 'SUDTIND-B.BO', 'PREMIER.BO', 'NKIND.BO', 'RCL.BO', 'GLOBUSSPR.BO', 'BLUECOAST.BO', 'ZENITHSTL.BO', 'SUPREMEINF.BO','NTL.BO',
             'PHOENIXTN.BO', 'FISCHER.BO'  ,'KLBRENG-B.BO' ,'BLUECLOUDS.BO'  ,'GVL.BO' ,'CALCOM.BO'  ,'ROYAL.BO', 'VENTURA.BO','JAGJANANI.BO','MANBRO.BO','VARYAA.BO','MONOT.BO','SREEJAYA.BO','BHUDEVI.BO','MEDINOV.BO','KOTIC.BO','SPICEISLIN.BO'
            ,'AANANDALAK.BO','HINDMOTORS.BO','KSOLVES.BO','NESTLEIND.BO','GUJTLRM.BO'
            ,'SBECSYS.BO'
            ,'GRETEX.BO'
            ,'AKIKO.BO'
            ,'VINTRON.BO'
            ,'SWADPOL.BO'
            ,'TUTIALKA.BO'
            ,'ASSOCIATED.BO'
            ,'DFPL.BO'
            ,'ATHENAGLO.BO'
            ,'SAHANA.BO'
            ,'NPST.BO'
            ,'SLONE.BO'
            ,'MAHAPEXLTD.BO'
            ,'AMBIT.BO'
            ,'FRANKLININD.BO'
            ,'DIGIKORE.BO'
            ,'VERITAAS.BO'
            ,'INFRONICS.BO'
            ,'DENTALKART.BO'
            ,'PRESSURS.BO'
            ,'TRIVENIGQ.BO'
            ,'SPECFOOD.BO'
            ,'KPGEL.BO'
            ,'TIPSINDLTD.BO'
            ,'WAAREERTL.BO'
            ,'SONAMAC.BO'
            ,'JFL.BO'
            ,'PGHH.BO'
            ,'RISHDIGA.BO'
            ,'REMLIFE.BO'
            ,'MEAPL.BO'
            ,'BRIDGESE.BO'
            ,'TECHNVISN.BO'
            ,'EXHICON.BO'
            ,'COLPAL.BO'
            ,'TECHKGREEN.BO'
            ,'VUENOW.BO'
            ,'RAJKSYN.BO'
            ,'GAYATRI.BO'
            ,'INFOLLION.BO'
            ,'OLATECH.BO'
            ,'MISHTANN.BO'
            ,'ENFUSE.BO'
            ,'WOMANCART.BO'
            ,'ONEGLOBAL.BO'
            ,'CELLECOR.BO'
            ,'RULKA.BO'
            ,'KRISHCA.BO'
            ,'TAC.BO'
            ,'AMIC.BO','QUESTLAB.BO'
            ,'ORIANA.BO'
            ,'ENSER.BO'
            ,'21STCENMGM.BO'
            ,'LLOYDSME.BO'
            ,'CRSTCHM.BO'
            ,'HIGHSTREE.BO'
            ,'ESABINDIA.BO','QUESTLAB.BO'
            ,'ORIANA.BO'
            ,'ENSER.BO'
            ,'21STCENMGM.BO'
            ,'LLOYDSME.BO'
            ,'CRSTCHM.BO'
            ,'HIGHSTREE.BO'
            ,'ESABINDIA.BO'
            ,'KALYANI.BO'
            ,'SUPREMEPWR.BO'
            ,'SHILCTECH.BO'
            ,'JSLL.BO'
            ,'CNCRD.BO'
            ,'PHCAP.BO'
            ,'LICI.BO'
            ,'PRIZOR.BO'
            ,'NEPHROCARE.BO'
            ,'SANOFI.BO'
            ,'ACCELERATE.BO'
            ,'ROCKINGDCE.BO'
            ,'SAIFL.BO'
            ,'IBINFO.BO'
            ,'VIPULLTD.BO'
            ,'NINSYS.BO'
            ,'YURANUS.BO'
            ,'TRL.BO'
            ,'PHANTOMFX.BO'
            ,'KESAR.BO'
            ,'TROM.BO','NEXUSSURGL.bo'
            ,'SDL.bo'
            ,'JYOTIRES.BO'
            ,'GEMENVIRO.BO'
            ,'SEL.BO'
            ,'COCHMAL.BO'
            ,'WINSOL.BO'
            ,'BALGOPAL.BO'
            ,'TCS.BO'
            ,'INA.BO'
            ,'COALINDIA.BO'
            ,'ZTECH.BO'
            ,'BNRUDY.BO'
            ,'PROMACT.BO'
            ,'DHRUVCA.BO'
            ,'KIDUJA.BO'
            ,'EVERFIN.BO'
            ,'TRUST.BO'
            ,'JAIBALAJI.BO'
            ,'FAALCON.BO'
            ,'EFACTOR.BO'
            ,'WANBURY.BO'
            ,'SUMUKA.BO','ACCENTMIC.BO','KEYCORP.BO','ACCELYA.BO'
            ,'KODYTECH.BO'
            ,'WEALTH.BO'
            ,'CASTROLIND.BO'
            ,'VINSYS.BO'
            ,'TOTEM.BO'
            ,'ASPIRE.BO'
            ,'SANJIVIN.BO'
            ,'ROBU.BO'
            ,'SBGLP.BO'
            ,'BRISK.BO'
            ,'ESCONET.BO'
            ,'GLOBAL.BO'
            ,'SHINEFASH.BO'
            ,'BAWEJA.BO'
            ,'IRCTC.BO'
            ,'PRIMIND.BO'
            ,'NETLINK.BO'
            ,'ALSL.BO'
            ,'KOTYARK.BO'
            ,'VISCO.BO'
            ,'AIIL.BO'
            ,'SIGMA.BO'
            ,'TUNWAL.BO'
            ,'PULZ.BO'
            ,'INM.BO'
            ,'FTL.BO'
            ,'DEEPAKCHEM.BO'
            ,'GILLETTE.BO'
            ,'WTICAB.BO'
            ,'SWARAJENG.BO'
            ,'KRONOX.BO'
            ,'MONARCH.BO'
            ,'DRONE.BO'
            ,'INGERRAND.BO'
            ,'GLAXO.BO'
            ,'OWAIS.BO'
            ,'NIKSTECH.BO'
            ,'SHELTER.BO'
            ,'APS.BO'
            ,'K2INFRA.BO'
            ,'DCM.BO'
            ,'ANANDRATHI.BO'
            ,'GOYALSALT.BO'
            ,'GCSL.BO'
            ,'COMCL.BO'
            ,'IEX.BO'
            ,'DELAPLEX.BO'
            ,'NEWJAISA.BO'
            ,'SATTRIX.BO'
            ,'BRITANNIA.BO'
            ,'HOACFOODS.BO','PCCL.BO'
            ,'ADVANIHOTR.BO'
            ,'SOUTHMG.BO'
            ,'CAMS.BO'
            ,'HBSL.BO'
            ,'SYSTMTXC.BO'
            ,'GBFL.BO'
            ,'MSUMI.BO'
            ,'TTML.BO'
            ,'PRATHAM.BO'
            ,'AVANTEL.BO'
            ,'SAI.BO'
            ,'NAMAN.BO'
            ,'OLIL.BO'
            ,'CGPOWER.BO'
            ,'SODFC.BO'
            ,'KESARENT.BO'
            ,'ZENTEC.BO'
            ,'TRADEWELL.BO'
            ,'JNKINDIA.BO'
            ,'HINDZINC.BO'
            ,'ESCORP.BO'
            ,'ASAL.BO'
            ,'YASHOPTICS.BO'
            ,'ABBOTINDIA.BO'
            ,'DREAMFOLKS.BO'
            ,'CLARA.BO'
            ,'ALUWIND.BO'
            ,'EFFWA.BO'
            ,'JAGSONFI.BO'
            ,'CYBERMEDIA.BO'
            ,'PGHL.BO'
            ,'VISHNUINFR.BO'
            ,'ADCINDIA.BO'
            ,'GANESHHOUC.BO'
            ,'PRITHVIEXCH.BO'
            ,'PAGEIND.BO'
            ,'GUJTHEM.BO'
            ,'PANORAMA.BO'
            ,'HSIL.BO'
            ,'HAWKINCOOK.BO'
            ,'EVANS.BO'
            ,'RSSOFTWARE.BO'
            ,'ZEAL.BO'
            ,'SHUKRAPHAR.BO'
            ,'DSSL.BO'
            ,'MAZDOCK.BO'
            ,'MESON.BO'
            ,'APARINDS.BO'
            ,'TAPARIA.BO'
            ,'GARGI.BO'
            ,'PRUDENT.BO','EFORCE.BO'
            ,'MARICO.BO'
            ,'INOXINDIA.BO'
            ,'DHOOTIN.BO'
            ,'TBOTEK.BO'
            ,'GCONNECT.BO'
            ,'UNIABEXAL.BO'
            ,'SHARDAMOTR.BO'
            ,'TATAELXSI.BO'
            ,'BEACON.BO'
            ,'MACOBSTECH.BO'
            ,'PARAGON.BO'
            ,'ACE.BO'
            ,'AKZOINDIA.BO'
            ,'ALPHAIND.BO'
            ,'SHREE.BO'
            ,'AEIM.BO'
            ,'CGRAPHICS.BO'
            ,'DDEVPLASTIK.BO'
            ,'MANAV.BO'
            ,'VADILENT.BO']  


all_stock_data = []

for symbol in bse_stock_symbols:
    try:
        stock_data = fetch_stock_data(symbol, start_epoch, end_epoch)
        stock_data['Symbol'] = symbol
        all_stock_data.append(stock_data)
        print(f"Fetched data for {symbol}")
        time.sleep(0.01)  
    except Exception as e:
        print(f"Could not fetch data for {symbol}. Error: {e}")


if all_stock_data:
    all_stock_data_df = pd.concat(all_stock_data, ignore_index=True)

    all_stock_data_df.to_csv('bse_historical_data.csv', index=False)
else:
    print("No data fetched for any stocks.")
