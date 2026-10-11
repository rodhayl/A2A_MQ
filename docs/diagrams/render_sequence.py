"""Render the reviewed sequence as editable SVG without remote rendering services."""
from html import escape
from pathlib import Path

root = Path(__file__).resolve().parent
W, H = 1540, 1200
xs = [95, 310, 525, 740, 955, 1170, 1385]
names = ['Client', 'API + relay', 'SQL', 'Vault', 'Dapr / broker', 'Worker', 'Observer']
parts = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}">',
'<defs><marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0L10 5L0 10z" fill="#254c77"/></marker></defs>',
'<rect width="100%" height="100%" fill="#f7f9fc"/>',
'<g font-family="DejaVu Sans, sans-serif" fill="#152d47">']
def text(x,y,s,size=13,anchor='middle'):
    parts.append(f'<text x="{x}" y="{y}" font-size="{size}" text-anchor="{anchor}">{escape(s)}</text>')
text(W/2,36,'Optional protected delivery | UML sequence',26)
for x,n in zip(xs,names):
    parts.append(f'<rect x="{x-85}" y="60" width="170" height="43" rx="5" fill="#e2ecf8" stroke="#53779e"/>')
    text(x,87,n,16)
    parts.append(f'<line x1="{x}" y1="103" x2="{x}" y2="1110" stroke="#9bacbf" stroke-dasharray="6 6"/>')
def arrow(a,b,y,label,dashed=False):
    x1,x2=xs[a],xs[b]
    if a==b:
        parts.append(f'<path d="M{x1} {y}h90v20h-90" fill="none" stroke="#254c77" marker-end="url(#arrow)"/>')
        if a == len(xs) - 1:
            text(x1-8,y-8,label,12,'end')
        else:
            text(x1+8,y-8,label,12,'start')
    else:
        dash='stroke-dasharray="5 4"' if dashed else ''
        parts.append(f'<line x1="{x1}" y1="{y}" x2="{x2}" y2="{y}" stroke="#254c77" {dash} marker-end="url(#arrow)"/>')
        text((x1+x2)/2,y-8,label)
def opt(y,h,label):
    parts.append(f'<rect x="200" y="{y}" width="1280" height="{h}" fill="#eee8f7" fill-opacity="0.65" stroke="#765593" stroke-dasharray="5 3"/>')
    text(212,y+19,'opt  '+label,13,'start')
arrow(0,1,140,'SendMessage + bearer')
arrow(1,2,182,'Commit task + publish action')
arrow(1,0,224,'SUBMITTED if returnImmediately',True)
opt(250,198,'crypto enabled; cache miss')
arrow(1,3,296,'Wrap fresh content key')
arrow(3,1,336,'wrappedKey',True)
arrow(1,1,377,'AES-GCM + authenticated header')
arrow(1,2,425,'Persist sealed bytes before publish')
arrow(1,4,480,'Publish command; sealed only when enabled')
arrow(4,1,522,'Publication accepted',True)
arrow(1,2,564,'Mark sent (crash window can duplicate)')
arrow(4,5,606,'Subscribed delivery')
opt(627,169,'crypto enabled')
arrow(5,3,674,'Unwrap content key')
arrow(3,5,714,'Content key',True)
arrow(5,5,754,'Authenticate + decrypt + validate')
arrow(5,2,832,'Durable admission / stable Workflow start')
arrow(5,4,878,'SUCCESS: admitted, not yet completed',True)
arrow(5,2,923,'Commit task result + outbox')
arrow(5,4,966,'Publish result; same optional protection')
arrow(4,6,1009,'Independent result subscription')
arrow(6,6,1054,'Decrypt if protected; project')
text(W/2,1140,'Protection ends at the applications. SQL and Workflow history remain outside scope.',17)
text(W/2,1171,'Visible authenticated header: version, algorithm, keyId, wrappedKey, nonce, tenant, receiver, route, messageId.',13)
parts.append('</g></svg>')
(root/'protected-sequence.svg').write_text('\n'.join(parts)+'\n')
