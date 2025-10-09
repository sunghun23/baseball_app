// Baseball statistics - app helpers
(function () {
  document.addEventListener('DOMContentLoaded', () => {
    attachDeleteConfirms();
    enhanceSearchInput();
    makeSelectSearchable('#player_id');
    makeSelectSearchable('#game_id');
  });

  // Add confirm dialog to any form with data-confirm or delete buttons
  function attachDeleteConfirms(){
    document.querySelectorAll('form[data-confirm], form button[data-confirm]').forEach(el=>{
      const target = el.tagName === 'FORM' ? el : el.closest('form');
      const msg = el.getAttribute('data-confirm') || '정말 삭제하시겠습니까?';
      if(!target._confirmBound){
        target.addEventListener('submit', (e)=>{
          if(!confirm(msg)) e.preventDefault();
        });
        target._confirmBound = true;
      }
    });
  }

  // Enhance /search page with simple client-side suggestions using /api/players
  function enhanceSearchInput(){
    const input = document.querySelector('form[action$="/search"] input[name="q"]');
    if(!input) return;

    // container
    const wrap = document.createElement('div');
    wrap.style.position = 'relative';
    input.parentElement.insertBefore(wrap, input);
    wrap.appendChild(input);

    const list = document.createElement('div');
    list.style.position='absolute';
    list.style.left='0'; list.style.right='0';
    list.style.top='100%';
    list.style.background='#fff';
    list.style.border='1px solid #e5e7eb';
    list.style.borderTop='0';
    list.style.borderRadius='0 0 12px 12px';
    list.style.boxShadow='0 10px 20px rgba(0,0,0,.06)';
    list.style.zIndex='1050';
    list.style.display='none';
    wrap.appendChild(list);

    let timer=null;
    input.addEventListener('input', ()=>{
      clearTimeout(timer);
      const q = input.value.trim();
      if(!q){ list.style.display='none'; list.innerHTML=''; return; }
      timer=setTimeout(async ()=>{
        try{
          const res = await fetch(`/api/players?q=${encodeURIComponent(q)}`);
          const data = await res.json();
          renderSuggestions(data);
        }catch(e){ /* ignore */ }
      }, 160);
    });

    document.addEventListener('click', (e)=>{
      if(!wrap.contains(e.target)) list.style.display='none';
    });

    function renderSuggestions(rows){
      list.innerHTML='';
      if(!rows.length){ list.style.display='none'; return;}
      rows.forEach(r=>{
        const a = document.createElement('a');
        a.href = `/player/${r.id}`;
        a.className='d-block px-3 py-2';
        a.style.color='#111827';
        a.innerHTML = `
          <div class="fw-semibold">${escapeHtml(r.name)}</div>
          <div class="small text-muted">${escapeHtml(r.team || '-')} · ${escapeHtml(r.position || '-')}</div>
        `;
        a.addEventListener('mouseenter', ()=>{ a.style.background='#f3f4f6'; });
        a.addEventListener('mouseleave', ()=>{ a.style.background='transparent'; });
        list.appendChild(a);
      });
      list.style.display='block';
    }
  }

  // Make a native <select> searchable (client-side filter)
  function makeSelectSearchable(selector){
    const select = document.querySelector(selector);
    if(!select) return;
    // wrapper
    const wrapper = document.createElement('div');
    wrapper.style.position = 'relative';
    select.parentElement.insertBefore(wrapper, select);
    wrapper.appendChild(select);

    // input
    const input = document.createElement('input');
    input.type='text';
    input.className='form-control mb-2';
    input.placeholder='검색해서 빠르게 찾기...';
    wrapper.insertBefore(input, select);

    const options = Array.from(select.options);
    input.addEventListener('input', ()=>{
      const q = input.value.trim().toLowerCase();
      select.innerHTML='';
      options.forEach(opt=>{
        const text = opt.textContent.toLowerCase();
        if(!q || text.includes(q)) select.appendChild(opt);
      });
    });
  }

  function escapeHtml(str){
    return String(str).replace(/[&<>"']/g, s=>({
      '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'
    }[s]));
  }
})();
