"""Render a fictional, conceptual ingestion diagram. No video or database access."""
from pathlib import Path
import math
from PIL import Image, ImageDraw, ImageFont

OUT=Path(__file__).resolve().parent
BG='#0B1220';WHITE='#F4F7FC';MUTED='#AAB9CD';BLUE='#60A5FA';TEAL='#2DD4BF';GOLD='#F2C66D';RED='#FB8D98'


def font(size,bold=False):
    for name in ['/System/Library/Fonts/Supplemental/Arial'+(' Bold' if bold else '')+'.ttf','DejaVuSans-Bold.ttf' if bold else 'DejaVuSans.ttf']:
        try: return ImageFont.truetype(name,size)
        except OSError: pass
    return ImageFont.load_default(size=size)


def text(d,x,y,s,size=22,color=WHITE,bold=False):
    d.text((x,y),s,font=font(size,bold),fill=color,anchor='mm')


def point(points,t):
    lengths=[math.dist(a,b) for a,b in zip(points,points[1:])];remaining=sum(lengths)*max(0,min(1,t))
    for i,length in enumerate(lengths):
        if remaining<=length:
            a,b=points[i:i+2];f=remaining/length
            return a[0]+(b[0]-a[0])*f,a[1]+(b[1]-a[1])*f
        remaining-=length
    return points[-1]


def arrow(d,points,color):
    d.line(points,fill=color,width=3,joint='curve')
    a,b=points[-2:];angle=math.atan2(b[1]-a[1],b[0]-a[0])
    d.polygon([b,(b[0]-11*math.cos(angle-.4),b[1]-11*math.sin(angle-.4)),(b[0]-11*math.cos(angle+.4),b[1]-11*math.sin(angle+.4))],fill=color)


nodes={
 'config':((80,205,1000,300),BLUE,'Parameter file',['Source + table  /  Columns + keys  /  Load mode + watermark']),
 'validate':((80,392,480,484),BLUE,'Validate configuration',['Required fields, identifiers, options']),
 'source':((600,392,1000,484),BLUE,'Read source data',['Use the configured source connector']),
 'load':((600,568,1000,666),GOLD,'Apply load strategy',['Full snapshot or incremental upsert']),
 'bronze':((80,568,480,666),TEAL,'Write Bronze data',['Persist the configured source records']),
 'success':((80,770,480,882),TEAL,'Advance checkpoint',['Only after a successful write']),
 'failure':((600,770,1000,882),RED,'Preserve checkpoint',['Retry eligible transient failures']),
 'audit':((80,944,1000,1018),BLUE,'Audit every attempt',['Table  /  Status  /  Row count  /  Timing  /  Error category'])
}
paths={
 'cv':([(540,301),(540,346),(280,346),(280,391)],BLUE),
 'vs':([(481,438),(599,438)],BLUE),
 'sl':([(800,485),(800,567)],BLUE),
 'lb':([(599,617),(481,617)],TEAL),
 'bs':([(280,667),(280,769)],TEAL),
 'sa':([(280,883),(280,943)],TEAL),
 'lf':([(800,667),(800,769)],RED),
 'fa':([(800,883),(800,943)],RED)
}
events=[('cv',.6,1.9,'validate'),('vs',2.4,3.4,'source'),('sl',3.9,4.9,'load'),('lb',5.4,6.4,'bronze'),('bs',6.9,7.9,'success'),('sa',8.3,9.1,'audit'),('sl',10.2,10.9,'load'),('lf',11.3,12.4,'failure'),('fa',12.9,13.8,'audit')]
stages=[(0,'Define what to ingest'),(1.9,'Validate before opening connections'),(3.4,'Extract data using the source connector'),(4.9,'Choose full or incremental loading'),(6.4,'Confirm the data write succeeded'),(7.9,'Advance checkpoint after success'),(9.1,'Record per-table operational evidence'),(10.1,'Separate example: the load fails'),(12.4,'Keep checkpoint unchanged; retry safely'),(13.8,'New table. New configuration. Same framework.')]
frames=[]
for frame in range(192):
    t=frame/12
    im=Image.new('RGB',(1080,1080),BG);d=ImageDraw.Draw(im)
    for i,c in enumerate([BLUE,TEAL,GOLD]):d.rectangle((i*360,0,(i+1)*360,6),fill=c)
    text(d,540,42,'A CONFIGURATION-DRIVEN REFERENCE DESIGN',16,MUTED,True)
    text(d,540,99,'Reusable Data Ingestion',47,WHITE,True)
    text(d,540,153,'New tables through configuration. Shared execution.',26,MUTED)
    for points,color in paths.values():arrow(d,points,'#3E536B' if color==BLUE else '#3A665E' if color==TEAL else '#714852')
    active={}
    for route,start,end,target in events:
        active[target]=max(active.get(target,0),math.exp(-((t-end)/.45)**2))
    if t<1:active['config']=math.exp(-((t-.4)/.4)**2)
    for name,(bounds,color,title,lines) in nodes.items():
        x1,y1,x2,y2=bounds;lit=active.get(name,0)>.2
        if lit:
            d.rounded_rectangle((x1-5,y1-5,x2+5,y2+5),radius=22,fill='#20314A')
        fill='#122337' if name not in ('success','failure') else '#112B2B' if name=='success' else '#2B1D2A'
        d.rounded_rectangle(bounds,radius=18,fill=fill,outline=color if lit or name in ('success','failure') else '#304359',width=3 if lit else 1)
        titley=y1+(27 if name=='audit' else 32)
        text(d,(x1+x2)/2,titley,title,26 if name not in ('audit',) else 23,color if lit or name in ('success','failure') else WHITE,True)
        for i,line in enumerate(lines):text(d,(x1+x2)/2,y1+(55 if name=='audit' else 67)+i*25,line,19 if name not in ('config',) else 20,MUTED)
    # Route labels have their own space and never sit on connector paths.
    text(d,169,739,'SUCCESS PATH',17,TEAL,True)
    text(d,911,739,'FAILURE PATH',17,RED,True)
    text(d,540,919,'Operational evidence across the entire flow',17,MUTED)
    caption=next(label for start,label in reversed(stages) if t>=start)
    text(d,540,1049,caption,22,WHITE,True)
    for route,start,end,target in events:
        if start<=t<=end:
            points,color=paths[route];p=(t-start)/(end-start)
            # Traveling packet and a dimmer trailing packet follow the connector.
            for offset,radius in [(0,6),(-.12,3)]:
                if p+offset<0:continue
                x,y=point(points,p+offset)
                d.ellipse((x-radius-3,y-radius-3,x+radius+3,y+radius+3),fill='#24364D')
                d.ellipse((x-radius,y-radius,x+radius,y+radius),fill=color)
    frames.append(im)
palette=frames[96].quantize(colors=128)
indexed=[im.quantize(palette=palette,dither=Image.Dither.NONE) for im in frames]
indexed[0].save(OUT/'reusable-ingestion-framework.gif',save_all=True,append_images=indexed[1:],duration=[80,80,90]*64,loop=0,disposal=2,optimize=True)
frames[96].save(OUT/'reusable-ingestion-framework.png')
print('GIF and static preview created.')
