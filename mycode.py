import requests
import pandas as pd
from datetime import datetime
import time
import io

def scrap_data(symbol, start_date, end_date):
    # Convert date strings to Unix timestamps
    period1 = int(time.mktime(datetime.strptime(start_date, "%Y-%m-%d").timetuple()))
    period2 = int(time.mktime(datetime.strptime(end_date, "%Y-%m-%d").timetuple()))

    # Construct the URL
    url = f"https://query1.finance.yahoo.com/v7/finance/download/{symbol}?period1={period1}&period2={period2}&interval=1d&events=history&includeAdjustedClose=true"

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    # Fetch the data
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        # Read the CSV data directly from the response content
        data = pd.read_csv(io.StringIO(response.text))
        data['Symbol'] = symbol
        # Process the data
        arr_data = []
        for _, row in data.iterrows():
            date = row['Date']
            open_price = row['Open']
            high = row['High']
            low = row['Low']
            close = row['Close']
            volume = row['Volume']
            symbol = row['Symbol']
            arr_data.append({
                'Date': date,
                'Open': open_price,
                'High': high,
                'Low': low,
                'Close': close,
                'Volume': volume,
                'Symbol': symbol
            })
        
        return pd.DataFrame(arr_data)  # Return DataFrame directly
    else:
        print(f"Failed to fetch data for {symbol} with status code: {response.status_code}")
        return None

def save_to_csv(data, cs):
    # Save the DataFrame to a CSV file
    data.to_csv(cs, index=False)
    print(f"Data saved to {cs}")

def process_stocks(symbols, start_date, end_date, output_filename):
    all_data = pd.DataFrame()
    
    for symbol in symbols:
        print(f"Fetching data for {symbol}...")
        data = scrap_data(symbol, start_date, end_date)
        if data is not None:
            all_data = pd.concat([all_data, data], ignore_index=True)
        else:
            print(f"Failed to fetch data for {symbol}")
    
    if not all_data.empty:
        save_to_csv(all_data, output_filename)
    else:
        print("No data fetched for any symbol.")

# Example usage
symbols = [ "RELIANCE.NS", "TCS.NS", "HDFCBANK.NS", "ICICIBANK.NS", "BHARTIARTL.NS", "SBIN.NS", 
            "INFY.NS", "LICI.NS", "ITC.NS", "HINDUNILVR.NS", "LT.NS", "BAJFINANCE.NS", "HCLTECH.NS", 
            "MARUTI.NS", "SUNPHARMA.NS", "ADANIENT.NS", "KOTAKBANK.NS", "TITAN.NS", "ONGC.NS", "TATAMOTORS.NS", 
            "NTPC.NS", "AXISBANK.NS", "DMART.NS", "ADANIGREEN.NS", "ADANIPORTS.NS", "ULTRACEMCO.NS", "ASIANPAINT.NS", 
            "COALINDIA.NS", "BAJAJFINSV.NS", "BAJAJ-AUTO.NS", "POWERGRID.NS", "NESTLEIND.NS", "WIPRO.NS", "M&M.NS", 
            "IOC.NS", "JIOFIN.NS", "HAL.NS", "DLF.NS", "ADANIPOWER.NS", "JSWSTEEL.NS", "TATASTEEL.NS", "SIEMENS.NS", 
            "IRFC.NS", "VBL.NS", "ZOMATO.NS", "PIDILITIND.NS", "GRASIM.NS", "SBILIFE.NS", "BEL.NS", "LTIM.NS", 
            "TRENT.NS", "PNB.NS", "INDIGO.NS", "BANKBARODA.NS", "HDFCLIFE.NS", "ABB.NS", "BPCL.NS", "PFC.NS",
            "GODREJCP.NS", "TATAPOWER.NS", "HINDALCO.NS", "HINDZINC.NS", "TECHM.NS", "AMBUJACEM.NS", "INDUSINDBK.NS", 
            "CIPLA.NS", "GAIL.NS", "RECLTD.NS", "BRITANNIA.NS", "UNIONBANK.NS", "ADANIENSOL.NS", "IOB.NS", 
            "LODHA.NS", "EICHERMOT.NS", "CANBK.NS", "TATACONSUM.NS", "DRREDDY.NS", "TVSMOTOR.NS", "ZYDUSLIFE.NS",
            "ATGL.NS", "VEDL.NS", "CHOLAFIN.NS", "HAVELLS.NS", "HEROMOTOCO.NS", "DABUR.NS", "SHREECEM.NS", 
            "MANKIND.NS", "BAJAJHLDNG.NS", "DIVISLAB.NS", "APOLLOHOSP.NS", "NHPC.NS", "SHRIRAMFIN.NS", "BOSCHLTD.NS", 
            "TORNTPHARM.NS", "ICICIPRULI.NS", "IDBI.NS", "JSWENERGY.NS", "JINDALSTEL.NS", "BHEL.NS", "INDHOTEL.NS", 
            "CUMMINSIND.NS", "ICICIGI.NS", "CGPOWER.NS", "MCDOWELL-N.NS", "HDFCAMC.NS", "MAXHEALTH.NS", "SOLARINDS.NS", 
            "MOTHERSON.NS", "INDUSTOWER.NS", "POLYCAB.NS", "OFSS.NS", "SRF.NS", "IRCTC.NS", "COLPAL.NS", "LUPIN.NS", 
            "NAUKRI.NS", "TIINDIA.NS", "INDIANB.NS", "HINDPETRO.NS", "BERGEPAINT.NS", "YESBANK.NS", "TORNTPOWER.NS", 
            "OIL.NS", "SBICARD.NS", "IDEA.NS", "MARICO.NS", "GODREJPROP.NS", "AUROPHARMA.NS", "UCOBANK.NS", "BANKINDIA.NS", 
            "PERSISTENT.NS", "MUTHOOTFIN.NS", "NMDC.NS", "ALKEM.NS", "PIIND.NS", "LTTS.NS", "GICRE.NS", "TATACOMM.NS", 
            "JSL.NS", "MRF.NS", "SAIL.NS", "PGHH.NS", "SUZLON.NS", "LINDEINDIA.NS", "SUPREMEIND.NS", "CONCOR.NS", 
            "OBEROIRLTY.NS", "ASTRAL.NS", "IDFCFIRSTB.NS", "RVNL.NS", "BHARATFORG.NS", "CENTRALBK.NS", "JSWINFRA.NS", 
            "POLICYBZR.NS", "ASHOKLEY.NS", "THERMAX.NS", "PHOENIXLTD.NS", "GMRINFRA.NS", "TATAELXSI.NS", "PATANJALI.NS", 
            "SJVN.NS", "PRESTIGE.NS", "ACC.NS", "NYKAA.NS", "SUNDARMFIN.NS", "UBL.NS", "ABCAPITAL.NS", "MPHASIS.NS",
            "BALKRISIND.NS", "DIXON.NS", "MAHABANK.NS", "KALYANKJIL.NS", "SCHAEFFLER.NS", "AWL.NS", "APLAPOLLO.NS", 
            "TATATECH.NS", "SONACOMS.NS", "KPITTECH.NS", "FACT.NS", "PSB.NS", "PETRONET.NS", "L&TFH.NS", "UNOMINDA.NS", 
            "PAGEIND.NS", "MRPL.NS", "AUBANK.NS", "MAZDOCK.NS", "HUDCO.NS", "GUJGASLTD.NS", "NIACL.NS", "CRISIL.NS", 
            "AIAENG.NS", "FEDERALBNK.NS", "IREDA.NS", "VOLTAS.NS", "DALBHARAT.NS", "POONAWALLA.NS", "MEDANTA.NS", 
            "IRB.NS", "3MINDIA.NS", "MFSL.NS", "M&MFIN.NS", "UPL.NS", "HONAUT.NS", "BSE.NS", "FLUOROCHEM.NS", 
            "COFORGE.NS", "LICHSGFIN.NS", "GLAXO.NS", "DELHIVERY.NS", "BDL.NS", "STARHEALTH.NS", "FORTIS.NS", 
            "BIOCON.NS", "COROMANDEL.NS", "NLCINDIA.NS", "TATAINVEST.NS", "JKCEMENT.NS", "IPCALAB.NS",
            "METROBRAND.NS", "KEI.NS", "ESCORTS.NS", "LLOYDSME.NS", "GLAND.NS", "IGL.NS", "NAM-INDIA.NS", 
            "APOLLOTYRE.NS", "JUBLFOOD.NS", "POWERINDIA.NS", "MSUMI.NS", "BANDHANBNK.NS", "DEEPAKNTR.NS",
            "ZFCVINDIA.NS", "AJANTPHARM.NS", "KPRMILL.NS", "SYNGENE.NS", "EIHOTEL.NS", "APARINDS.NS", 
            "NATIONALUM.NS", "TATACHEM.NS", "GLENMARK.NS", "HINDCOPPER.NS", "GODREJIND.NS", "NH.NS", 
            "BLUESTARCO.NS", "EXIDEIND.NS", "ENDURANCE.NS", "JBCHEPHARM.NS", "PAYTM.NS", "ANGELONE.NS", 
            "MOTILALOFS.NS", "ITI.NS", "360ONE.NS", "CARBORUNIV.NS", "AARTIIND.NS", "SUNTV.NS", "KIOCL.NS",
            "ISEC.NS", "RADICO.NS", "SUNDRMFAST.NS", "CREDITACC.NS", "COCHINSHIP.NS", "HATSUN.NS", "MANYAVAR.NS",
            "CYIENT.NS", "GET&D.NS", "BRIGADE.NS", "TIMKEN.NS", "NBCC.NS", "JBMA.NS", "GILLETTE.NS", "KANSAINER.NS", 
            "LAURUSLABS.NS", "GRINDWELL.NS", "FIVESTAR.NS", "SWANENERGY.NS", "CHOLAHLDNG.NS", "IRCON.NS", "SKFINDIA.NS",
            "BSOFT.NS", "ASTERDM.NS", "RELAXO.NS", "SONATSOFTW.NS", "GSPL.NS", "RATNAMANI.NS", "ABFRL.NS", "APLLTD.NS", 
            "PFIZER.NS", "RAMCOCEM.NS", "SIGNATURE.NS", "PEL.NS", "ELGIEQUIP.NS", "LALPATHLAB.NS", "EMAMILTD.NS", "SANOFI.NS", 
            "JYOTICNC.NS", "TRIDENT.NS", "CASTROLIND.NS", "KAJARIACER.NS", "KAYNES.NS", "CENTURYTEX.NS", "CHALET.NS", "DEVYANI.NS", 
            "CDSL.NS", "KEC.NS", "SCHNEIDER.NS", "IDFC.NS", "BATAINDIA.NS", "CIEINDIA.NS", "KPIL.NS", "RRKABEL.NS"
            "SUMICHEM.NS", "NATCOPHARM.NS", "SUVENPHAR.NS", "CROMPTON.NS", "TRITURBINE.NS", 
            "PPLPHARMA.NS", "INOXWIND.NS", "ACE.NS", "ATUL.NS", "CGCL.NS", "TVSHLTD.NS", 
            "SHYAMMETL.NS", "NUVAMA.NS", "KIMS.NS", "CELLO.NS", "PNBHOUSING.NS", "REDINGTON.NS", 
            "LAXMIMACH.NS", "JYOTHYLAB.NS", "CESC.NS", "GODFRYPHLP.NS", "NSLNISP.NS", "RITES.NS", 
            "CONCORDBIO.NS", "INDIAMART.NS", "AEGISCHEM.NS", "OLECTRA.NS", "WHIRLPOOL.NS", 
            "ANANDRATHI.NS", "NAVINFLUOR.NS", "JWL.NS", "APTUS.NS", "FINCABLES.NS", "FINPIPE.NS", 
            "POLYMED.NS", "VINATIORGA.NS", "INTELLECT.NS", "JAIBALAJI.NS", "J&KBANK.NS", 
            "KARURVYSYA.NS", "BLUEDART.NS", "MANAPPURAM.NS", "AFFLE.NS", "NCC.NS", "RBLBANK.NS", 
            "TTML.NS", "BASF.NS", "VGUARD.NS", "CAMS.NS", "GESHIP.NS", "CENTURYPLY.NS", "CLEAN.NS", 
            "JINDALSAW.NS", "FSL.NS", "ZENSARTECH.NS", "SOBHA.NS", "CHAMBLFERT.NS", "DATAPATTNS.NS", 
            "CHENNPETRO.NS", "WELCORP.NS", "MGL.NS", "KSB.NS", "WELSPUNLIV.NS", "HSCL.NS", 
            "DCMSHRIRAM.NS", "ASTRAZEN.NS", "ZEEL.NS", "BEML.NS", "HFCL.NS", "RAINBOW.NS", 
            "ABSLAMC.NS", "HONASA.NS", "ASAHIINDIA.NS", "PVRINOX.NS", "ARE&M.NS", "IIFL.NS", 
            "BLS.NS", "ALOKINDS.NS", "VTL.NS", "GRINFRA.NS", "HBLPOWER.NS", "WESTLIFE.NS", 
            "RKFORGE.NS", "KIRLOSENG.NS", "TITAGARH.NS", "FINEORG.NS", "AMBER.NS", "BIKAJI.NS", 
            "SWSOLAR.NS", "RAYMOND.NS", "IEX.NS", "SPARC.NS", "GRAPHITE.NS", "SPLPETRO.NS", 
            "RAILTEL.NS", "INGERRAND.NS", "ECLERX.NS", "JUNIPER.NS", "ERIS.NS", "RHIM.NS", 
            "ENGINERSIN.NS", "MAHSEAMLES.NS", "HAPPSTMNDS.NS", "JKTYRE.NS", "TEJASNET.NS", 
            "PNCINFRA.NS", "NEWGEN.NS", "INOXINDIA.NS", "TANLA.NS", "BIRLACORPN.NS", "BBTC.NS", 
            "GMDCLTD.NS", "NUVOCO.NS", "AKZOINDIA.NS", "CEATLTD.NS", "RPOWER.NS", "RELINFRA.NS", 
            "GPIL.NS", "ELECON.NS", "ANANTRAJ.NS", "ELECTCAST.NS", "DBREALTY.NS", "EQUITASBNK.NS", 
            "KFINTECH.NS", "BAJAJELEC.NS", "LATENTVIEW.NS", "JPPOWER.NS", "GRANULES.NS", "AAVAS.NS", 
            "AETHER.NS", "UTIAMC.NS", "LEMONTREE.NS", "JKLAKSHMI.NS", "GPPL.NS", "SFL.NS", "PCBL.NS", 
            "MAPMYINDIA.NS", "ROUTE.NS", "CANFINHOME.NS", "CUB.NS", "SAPPHIRE.NS", "CAPLIPOINT.NS", 
            "MINDACORP.NS", "MMTC.NS", "PTCIL.NS", "IFCI.NS", "PRAJIND.NS", "VOLTAMP.NS", "SCI.NS", 
            "USHAMART.NS", "EIDPARRY.NS", "RTNINDIA.NS", "ANURAS.NS", "GLS.NS", "DOMS.NS", "INFIBEAM.NS", 
            "FORCEMOT.NS", "ZYDUSWELL.NS", "STARCEMENT.NS", "GODREJAGRO.NS", "TTKPRESTIG.NS", 
            "ALKYLAMINE.NS", "GNFC.NS", "KPIGREEN.NS", "CRAFTSMAN.NS", "MAHLIFE.NS", "REDTAPE.NS", 
            "JUBLPHARMA.NS", "NETWEB.NS", "NETWORK18.NS", "PRSMJOHNSN.NS", "METROPOLIS.NS", "CERA.NS", 
            "SBFC.NS", "GRSE.NS", "KIRLOSBROS.NS", "UJJIVANSFB.NS", "SHRIPISTON.NS", "RENUKA.NS", 
            "RATEGAIN.NS", "WOCKPHARMA.NS", "SAFARI.NS", "HAPPYFORGE.NS", "TECHNOE.NS", "SHOPERSTOP.NS", 
            "IBULHSGFIN.NS", "SYRMA.NS", "TEGA.NS", "ACI.NS", "MEDPLUS.NS", "MAHSCOOTER.NS", 
            "NEULANDLAB.NS", "AZAD.NS", "ESABINDIA.NS", "GALAXYSURF.NS", "ZENTEC.NS", "JSWHL.NS", 
            "TV18BRDCST.NS", "HOMEFIRST.NS", "MHRIL.NS", "POWERMECH.NS", "KTKBANK.NS", "JLHL.NS", 
            "MASTEK.NS", "PGHL.NS", "THOMASCOOK.NS", "CCL.NS", "GSFC.NS", "RAJESHEXPO.NS", "QUESS.NS", 
            "VARROC.NS", "TMB.NS", "MANINFRA.NS", "EASEMYTRIP.NS", "VIPIND.NS", "IONEXCHANG.NS", 
            "RESPONIND.NS", "MIDHANI.NS", "EMIL.NS", "GAEL.NS", "BALRAMCHIN.NS", "STAR.NS", 
            "JUBLINGREA.NS", "SARDAEN.NS", "JMFINANCIL.NS", "SOUTHBANK.NS", "HEG.NS", "CHEMPLASTS.NS", 
            "ARVIND.NS", "RCF.NS", "NAVA.NS", "ALLCARGO.NS", "ICIL.NS", "IWEL.NS", "KNRCON.NS", "FDC.NS", 
            "RELIGARE.NS", "GRAVITA.NS", "RUSTOMJEE.NS", "MARKSANS.NS", "NIITMTS.NS", "AHLUCONT.NS", 
            "JUSTDIAL.NS", "TRIVENI.NS", "TVSSCS.NS", "GARFIBRES.NS", "VESUVIUS.NS", "SAREGAMA.NS", 
            "DBL.NS", "INDIASHLTR.NS", "BLUEJET.NS", "BALAMINES.NS", "ISGEC.NS", "AVANTIFEED.NS", 
            "INDIACEM.NS", "BECTORFOOD.NS", "CAMPUS.NS", "LTFOODS.NS", "VIJAYA.NS", "GOCOLORS.NS", 
            "BORORENEW.NS", "LXCHEM.NS", "GREENLAM.NS", "DEEPAKFERT.NS", "CMSINFO.NS", "KRBL.NS", 
            "ETHOSLTD.NS", "TEXRAIL.NS", "TCI.NS", "IBREALEST.NS", "JINDWORLD.NS", "EMUDHRA.NS", "PDSL.NS", 
            "GANESHHOUC.NS", "CSBBANK.NS", "SHAREINDIA.NS", "IFBIND.NS", "PRINCEPIPE.NS", "VAIBHAVGBL.NS", 
            "ARVINDFASN.NS", "EDELWEISS.NS", "SENCO.NS", "SPANDANA.NS", "INDIGOPNTS.NS", "GENUSPOWER.NS",
            "SYMPHONY.NS", "HGINFRA.NS", "TIPSINDLTD.NS", "SIS.NS", "MSTCLTD.NS", "NESCO.NS", "SANGHVIMOV.NS", 
            "SANDUMA.NS", "UJJIVAN.NS", "ITDCEM.NS", "CYIENTDLM.NS", "EPL.NS", "SUPRAJIT.NS", "SUNTECK.NS", 
            "HEMIPROP.NS", "MOIL.NS", "TIMETECHNO.NS", "ASTRAMICRO.NS", "TRIL.NS", "WONDERLA.NS", "ASKAUTOLTD.NS", 
            "LLOYDSENGG.NS", "GMMPFAUDLR.NS", "SURYAROSNI.NS", "VSTIND.NS", "PTC.NS", "JKPAPER.NS", "SANSERA.NS",
            "CHOICEIN.NS", "AURIONPRO.NS", "PAISALO.NS", "ITDC.NS", "HNDFDS.NS", "PARADEEP.NS", "KESORAMIND.NS",
            "HCC.NS", "ORCHPHARMA.NS", "JAMNAAUTO.NS", "ICRA.NS", "RSYSTEMS.NS", "PRUDENT.NS", "MTARTECH.NS", 
            "UTKARSHBNK.NS", "RAIN.NS", "DYNAMATECH.NS", "JAICORPLTD.NS", "RBA.NS", "GATEWAY.NS", "PURVA.NS",
            "GUJALKALI.NS", "NAZARA.NS", "RALLIS.NS", "VRLLOG.NS", "GABRIEL.NS", "DODLA.NS", "JKIL.NS", "ROLEXRINGS.NS", 
            "WABAG.NS", "PRICOLLTD.NS", "HCG.NS", "AGI.NS", "DBCORP.NS", "FUSION.NS", "DHANUKA.NS", "MASFIN.NS", "SULA.NS", 
            "TDPOWERSYS.NS", "GALLANTT.NS", "JAYNECOIND.NS", "GULFOILLUB.NS", "SAMHI.NS", "TEAMLEASE.NS", "KIRLPNU.NS", "EPIGRAL.NS", 
            "TIIL.NS","GOPAL.NS", "JTEKTINDIA.NS", "HEIDELBERG.NS", "SUNDARMHLD.NS", "RTNPOWER.NS", "STLTECH.NS", "JPASSOCIAT.NS", 
            "PATELENG.NS", "ASHOKA.NS", "SINDHUTRAD.NS", "PGEL.NS", "NFL.NS", "ENTERO.NS", "JSFB.NS", "GOKEX.NS", "BANCOINDIA.NS", 
            "VMART.NS", "SHANTIGEAR.NS", "GHCL.NS", "SUDARSCHEM.NS", "WELENT.NS", "FEDFINA.NS", "NOCIL.NS", "TARC.NS", "KKCL.NS", 
            "ORIENTELEC.NS", "BOROLTD.NS", "KIRLOSIND.NS", "BALMLAWRIE.NS", "FCL.NS", "GRWRHITECH.NS", "SHARDAMOTR.NS", "PARKHOTELS.NS", 
            "MAXESTATES.NS", "TI.NS", "AMIORG.NS", "ORIENTCEM.NS", "SHILPAMED.NS", "AARTIDRUGS.NS", "LGBBROSLTD.NS", "AARTIPHARM.NS", 
            "TCIEXP.NS", "WSTCSTPAPR.NS", "ADVENZYMES.NS", "PRIVISCL.NS", "GREENPANEL.NS", "VENUSPIPES.NS", "BBOX.NS", "IIFLSEC.NS", 
            "PILANIINVS.NS", "ROSSARI.NS", "KSL.NS", "DCBBANK.NS", "IMAGICAA.NS", "BAJAJHIND.NS", "DCAL.NS", "HARSHA.NS", "BBL.NS", 
            "YATHARTH.NS", "ORISSAMINE.NS", "THANGAMAYL.NS", "ZAGGLE.NS", "BHARATRAS.NS", "KOLTEPATIL.NS", "KSCL.NS", "MEDIASSIST.NS", 
            "INOXGREEN.NS", "HATHWAY.NS", "SSWL.NS", "UNICHEMLAB.NS", "CIGNITITEC.NS", "IMFA.NS", "ASHAPURMIN.NS", "HGS.NS", 
            "MUTHOOTMF.NS", "SUBROS.NS", "RAMKY.NS", "SUNFLAG.NS", "CARERATING.NS", "GENSOL.NS", "SKIPPER.NS"
           
]  # List of stock symbols
start_date = '2015-01-01'
end_date = '2024-06-30'
output_filename = 'multiple_companies_data.csv'
process_stocks(symbols, start_date, end_date, output_filename)