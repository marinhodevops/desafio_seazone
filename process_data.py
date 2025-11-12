import os, random
from datetime import datetime
from pathlib import Path
import pandas as pd, numpy as np, requests
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas

OUT_DIR = Path(__file__).parent / "output"
OUT_DIR.mkdir(exist_ok=True)

def simulate_data(month):
    props, bookings, feedbacks, financials = [], [], [], []
    cities = ["São Paulo","Rio de Janeiro","Salvador","Fortaleza","Recife"]
    regions = {"São Paulo":"Sudeste","Rio de Janeiro":"Sudeste","Salvador":"Nordeste","Fortaleza":"Nordeste","Recife":"Nordeste"}
    for i in range(1,31):
        pid = f"P{i:04d}"; city = random.choice(cities)
        props.append({"property_id":pid,"owner_name":f"Owner {i}","city":city,"state":"BR","region":regions[city],"status":"active"})
        num_res = random.randint(0,25); gross = round(random.uniform(1000,15000),2) if num_res>0 else 0
        occupied = random.randint(0,25)
        bookings.append({"property_id":pid,"month":month,"num_reservations":num_res,"gross_revenue":gross,"occupied_days":occupied})
        fee = 15 if city=="São Paulo" else (12 if city=="Rio de Janeiro" else 18)
        extra = random.choice([0,0,0,150,0,1200])
        net = round(gross - (gross*fee/100) - extra,2)
        financials.append({"property_id":pid,"month":month,"platform_fee_pct":fee,"extra_cost":extra,"gross_revenue":gross,"net_revenue":net})
        avg = round(random.uniform(2.5,5.0),2) if num_res>0 else None
        complaints = {"limpeza": random.randint(0,5),"manutencao": random.randint(0,3),"checkin": random.randint(0,2)}
        feedbacks.append({"property_id":pid,"month":month,"avg_rating":avg,"complaint_categories":complaints})
    return props, bookings, feedbacks, financials

def generate_outputs(props, bookings, feedbacks, financials, month):
    dfp = pd.DataFrame(props)
    dfb = pd.DataFrame(bookings)
    dff = pd.DataFrame(financials)
    dffb = pd.DataFrame(feedbacks)

    dfb = dfb.rename(columns={"gross_revenue": "gross_booking_revenue"})
    dff = dff.rename(columns={"gross_revenue": "gross_financial_revenue"})

    df = dfp.merge(dfb, on="property_id").merge(dff, on=["property_id", "month"], how="left")

    df["gross_revenue"] = df["gross_financial_revenue"].fillna(df["gross_booking_revenue"])
    df["occupancy_pct"] = (df["occupied_days"]/30*100).round(2)
    df["margin_pct"] = np.where(df["gross_revenue"]>0,(df["net_revenue"]/df["gross_revenue"]*100).round(2),None)

    csv_path = OUT_DIR / f"consolidated_{month}.csv"
    df.to_csv(csv_path, index=False)
    if not dffb.empty:
        dffb.to_csv(OUT_DIR / f"feedbacks_{month}.csv", index=False)

    pdf_path = OUT_DIR / f"relatorio_financeiro_{month}.pdf"
    c = canvas.Canvas(str(pdf_path), pagesize=A4)
    w,h = A4
    c.setFont("Helvetica-Bold",16)
    c.drawString(40,h-60,f"Relatório Mensal {month}")
    c.setFont("Helvetica",10)
    c.drawString(40,h-80,f"Gerado em {datetime.now():%Y-%m-%d %H:%M}")
    c.setFont("Helvetica-Bold",12)
    c.drawString(40,h-110,"Top 5 imóveis por faturamento bruto")
    c.setFont("Helvetica",10)
    y=h-130
    for _,r in df.sort_values("gross_revenue",ascending=False).head(5).iterrows():
        c.drawString(42,y,f"{r.property_id} - {r.owner_name} - R$ {r.gross_revenue:.2f} - Margem {r.margin_pct or 0}%")
        y-=14
    y-=20
    c.setFont("Helvetica-Bold",12)
    c.drawString(40,y,"KPIs Consolidados")
    y-=18
    c.setFont("Helvetica",10)
    c.drawString(42,y,f"Receita Bruta Total: R$ {df.gross_revenue.sum():.2f}")
    y-=14
    c.drawString(42,y,f"Receita Líquida Total: R$ {df.net_revenue.sum():.2f}")
    y-=14
    c.drawString(42,y,f"Ocupação Média: {df.occupancy_pct.mean():.2f}%")
    c.showPage()
    c.save()
    print("[INFO] CSV:", csv_path)
    print("[INFO] PDF:", pdf_path)
    return pdf_path

def notify_discord(msg):
    url=os.getenv("DISCORD_WEBHOOK_URL")
    if not url:
        print("[WARN] Webhook não configurado.")
        return
    try:
        r=requests.post(url,json={"content":msg},timeout=10)
        print("[INFO] Discord:",r.status_code)
    except Exception as e:
        print("[ERROR] Discord:",e)

def main():
    month=datetime.now().strftime("%Y-%m")
    props,bookings,feedbacks,financials=simulate_data(month)
    pdf=generate_outputs(props,bookings,feedbacks,financials,month)
    notify_discord(f"Fechamento mensal {month} concluído. Relatório: {pdf.name}")
    print("[INFO] Done.")

if __name__=="__main__": main()
