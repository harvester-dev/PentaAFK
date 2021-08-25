import plotly.express as px
import plotly.graph_objects as go
from analysis import *
from datetime import date, datetime

keyword = "pentawafk"
dataFilesPath = os.path.join('data','raw')
onlyfiles = [f for f in os.listdir(dataFilesPath) if os.path.isfile(os.path.join(dataFilesPath, f))]

dates = []
vodids = []
countKeywords = []
countTotals = []
countPercentages = []

for path in onlyfiles:
    vodid = os.path.splitext(path)[0]
    print(vodid)
    data = analyze(vodid,keyword)

    vodDate = getVodDate(vodid).date()
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=data.index, y=data[keyword+'Normalized'], name="hv",
                        line_shape='hv'))
    fig.update_layout(
        title=str(vodDate)+' (VOD: '+vodid+')',
        yaxis_title="AFK (1=Yes, 0=No)",
        xaxis_title="VOD time (UTC)",
        # legend_title="Legend Title",
        # font=dict(
        #     family="Courier New, monospace",
        #     size=18,
        #     color="RebeccaPurple"
        # )
    )
    # fig.show()
    html=fig.write_html(file=os.path.join('html',vodid+".html"),include_plotlyjs='cdn')
    
    dates.append(vodDate)
    vodids.append(vodid)
    counts= countTime(data, keyword)
    countKeywords.append(counts[0])
    countTotals.append(counts[1])
    countPercentages.append((counts[0]/counts[1])*100)
    
summary = pd.DataFrame({'date': dates,'vodid': vodids, 'countKeyword': countKeywords, 'countTotal': countTotals, 'countPercentage': countPercentages })
summary.to_csv(os.path.join('html','summary.csv'))